package ru.quipy.payments.logic

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.ceil
import kotlin.time.Duration.Companion.nanoseconds

@Service
class OrderPayer(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    meterRegistry: MeterRegistry,
    environment: Environment,
) {

    companion object {
        private const val DEADLINE_USAGE_RATIO = 0.75
        private const val QUEUE_WAIT_SECONDS = 1.0
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val parallelRequests = paymentAccounts.minOf { it.properties.parallelRequests }
    private val averageProcessingTimeSeconds = paymentAccounts.sumOf {
        it.properties.averageProcessingTime.toMillis() / 1000.0
    }

    private val processingTimeReserveSeconds = 2.0

    private val processingRate = minOf(
        paymentAccounts.minOf { it.properties.rateLimitPerSec }.toDouble(),
        parallelRequests / averageProcessingTimeSeconds,
    )

    private var pendingPaymentsCount = 0

    private val paymentThreads = ceil(processingRate * processingTimeReserveSeconds)
        .toInt().coerceIn(1, parallelRequests)
    private val queueCapacity = ceil(processingRate * QUEUE_WAIT_SECONDS).toInt().coerceAtLeast(1)

    private val paymentRateLimiter = RateLimiter.of(
        "incoming-payments",
        RateLimiterConfig.custom()
            .limitForPeriod(paymentAccounts.minOf { it.properties.rateLimitPerSec })
            .limitRefreshPeriod(Duration.ofMillis(1020))
            .build()
    )

    private val paymentExecutor = ThreadPoolExecutor(
        paymentThreads,
        paymentThreads,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue<Runnable>(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    private val paymentQueueSize = Gauge.builder(
        "payment.queue.size",
        paymentExecutor.queue) { it.size.toDouble() }
            .tag("service", environment.getRequiredProperty("payment.service-name"))
            .register(meterRegistry)

    suspend fun awaitPaymentPermission(deadline: Long): Boolean {
        val remainingMillis = deadline - System.currentTimeMillis()
        if (remainingMillis < processingTimeReserveSeconds * 1000) return false

        val timeoutMillis = (remainingMillis - (processingTimeReserveSeconds + QUEUE_WAIT_SECONDS) * 1000)
            .toLong().coerceAtLeast(0)
        val waitNanos = synchronized(paymentRateLimiter) {
            paymentRateLimiter.changeTimeoutDuration(Duration.ofMillis(timeoutMillis))
            paymentRateLimiter.reservePermission()
        }
        if (waitNanos < 0) return false

        delay(waitNanos.nanoseconds)
        return true
    }

    @Synchronized
    fun tryReservePayment(deadline: Long): Boolean {
        if (pendingPaymentsCount >= paymentThreads + queueCapacity) return false

        val remainingSeconds = (deadline - System.currentTimeMillis()) / 1000.0
        val estimatedCompletionSeconds = maxOf(
            averageProcessingTimeSeconds,
            (pendingPaymentsCount + 1) / processingRate,
        )
        if (estimatedCompletionSeconds > remainingSeconds * DEADLINE_USAGE_RATIO) return false

        pendingPaymentsCount++
        return true
    }

    @Synchronized
    fun releasePayment() {
        pendingPaymentsCount--
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                releasePayment()
            }
        }
        return createdAt
    }
}