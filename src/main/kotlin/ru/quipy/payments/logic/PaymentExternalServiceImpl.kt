package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    override val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    private companion object {
        const val MAX_ATTEMPTS = 4
        const val TIMEOUT_PERCENTILE = 0.8
        const val TIMEOUT_MARGIN = 1.2

        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1020))
    private val semaphore = Semaphore(parallelRequests, true)
    private var waitingRetries = 0

    private val paymentRetry = Retry.of(
        "payment-$accountName",
        RetryConfig.custom<Boolean>()
            .maxAttempts(MAX_ATTEMPTS)
            .waitDuration(Duration.ZERO)
            .retryOnResult { retryRequired -> retryRequired && !Thread.currentThread().isInterrupted }
            .retryOnException { false }
            .build()
    )

    private val paymentRetries = meterRegistry.counter(
        "payment.external.retries",
        "service", serviceName,
        "accountName", accountName,
    )

    private val paymentRequestDuration = Timer.builder("payment.external.duration")
        .tag("service", serviceName)
        .tag("accountName", accountName)
        .publishPercentiles(0.5, TIMEOUT_PERCENTILE, 0.9, 0.95, 0.99)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        var attempt = 0
        paymentRetry.executeSupplier {
            performPaymentAttempt(paymentId, amount, paymentStartedAt, deadline, ++attempt)
        }
    }

    private fun performPaymentAttempt(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
    ): Boolean {
        if (now() >= deadline || Thread.currentThread().isInterrupted) return false

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var requestTimedOut = false
        try {
            semaphore.acquire()
            try {
                if (attempt > 1) synchronized(rateLimiter) { waitingRetries++ }
                try {
                    while (true) {
                        val remainingMillis = deadline - now()
                        if (remainingMillis <= 0) throw SocketTimeoutException("Payment deadline exceeded")
                        val permitted = synchronized(rateLimiter) {
                            (attempt > 1 || waitingRetries == 0) && rateLimiter.tick()
                        }
                        if (permitted) break
                        Thread.sleep(minOf(10L, remainingMillis))
                    }
                } finally {
                    if (attempt > 1) synchronized(rateLimiter) { waitingRetries-- }
                }

                val timeout = requestTimeout()
                val request = Request.Builder()
                    .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$timeout")
                    .post(emptyBody)
                    .build()

                val call = client.newCall(request)
                call.timeout().timeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                call.timeout().deadlineNanoTime(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadline - now()))
                if (attempt > 1) paymentRetries.increment()
                val requestStartedAt = System.nanoTime()
                val (responseCode, responseBody) = try {
                    call.execute().use { response ->
                        response.code to response.body?.string()
                    }
                } catch (e: InterruptedIOException) {
                    requestTimedOut = !Thread.currentThread().isInterrupted &&
                        (e is SocketTimeoutException || e.message == "timeout")
                    throw e
                } finally {
                    paymentRequestDuration.record(System.nanoTime() - requestStartedAt, TimeUnit.NANOSECONDS)
                }

                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $responseCode, reason: $responseBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, attempt: $attempt, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                return !body.result && (responseCode == 408 ||
                    responseCode in 200..299 && body.message == "Temporary error")
            } finally {
                semaphore.release()
            }
        } catch (e: Exception) {
            when {
                requestTimedOut || e is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            if (e is InterruptedException) Thread.currentThread().interrupt()
            return requestTimedOut
        }
    }

    private fun requestTimeout(): Duration {
        val averageNanos = requestAverageProcessingTime.toNanos().toDouble()
        val percentile = paymentRequestDuration.takeSnapshot().percentileValues()
            .firstOrNull { it.percentile() == TIMEOUT_PERCENTILE }
            ?.value(TimeUnit.NANOSECONDS)
            ?.takeIf { it.isFinite() && it > 0 } ?: averageNanos
        return Duration.ofNanos(ceil(maxOf(percentile, averageNanos) * TIMEOUT_MARGIN).toLong().coerceAtLeast(1))
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
