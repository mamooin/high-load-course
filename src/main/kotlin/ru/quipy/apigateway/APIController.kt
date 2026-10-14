package ru.quipy.apigateway

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*

@RestController
class APIController(
    meterRegistry: MeterRegistry,
    environment: Environment,
) {

    private val paymentRequests = meterRegistry.counter(
        "payment.requests",
        "service",
        environment.getRequiredProperty("payment.service-name")
    )

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        paymentRequests.increment()
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)
            ?: throw IllegalArgumentException("No such order $orderId")
        if (!orderPayer.pendingPayments.tryAcquire()) {
            throw ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Payment capacity exhausted")
        }
        val createdAt = try {
            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        } catch (e: Exception) {
            orderPayer.pendingPayments.release()
            throw e
        }
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}