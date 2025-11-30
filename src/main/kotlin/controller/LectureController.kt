package org.example.controller

import org.example.avro.AvroOrderEventProducer
import org.example.basic.OrderEventPublisher
import org.example.model.CreateOrderRequest
import org.example.model.OrderEvent
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.util.UUID


@RestController
@RequestMapping("/api/lecture")
class LectureController(
    private val orderEventPublisher: OrderEventPublisher,
    private val avroEventPublisher: AvroOrderEventProducer,
) {

    @PostMapping
    fun createOrder(@RequestBody request : CreateOrderRequest) : ResponseEntity<String> {
        val orderEvent = OrderEvent(
            orderId = UUID.randomUUID().toString(),
            customerId = request.customerId,
            quantity = request.quantity,
            price = request.price,
        )

        orderEventPublisher.publishOrderEvent(orderEvent)

        return ResponseEntity.ok("Order created")
    }

    @PostMapping("/avro/publish")
    fun createOrder(
        @RequestParam(defaultValue = "CUST-123") customerId: String,
        @RequestParam(defaultValue = "5") quantity: Int,
        @RequestParam(defaultValue = "99.99") price: BigDecimal
    ): Map<String, Any> {

        val orderId = UUID.randomUUID().toString()

        avroEventPublisher.publishOrderEvent(
            orderId = orderId,
            customerId = customerId,
            quantity = quantity,
            price = price
        )

        return mapOf(
            "success" to true,
            "orderId" to orderId,
            "message" to "Avro order event published successfully"
        )
    }


}