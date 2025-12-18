package org.example.controller

import org.example.avro.AvroOrderEventProducer
import org.example.basic.OrderEventPublisher
import org.example.model.CreateOrderRequest
import org.example.model.OrderCountComparisonStats
import org.example.model.OrderEvent
import org.example.model.StatsResponse
import org.example.stream.AvroOrderStreamsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
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
    private val avroOrderStreamsService: AvroOrderStreamsService,
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


//    서버가 자동으로 orderId 생성
//
//    customerId, quantity, price 를 파라미터로 받음
//
//    이 데이터로 Avro GenericRecord를 만듦
//    → Avro 스키마(order-entity.avsc) 기반
//
//    즉, Avro 레코드를 생성하는 시작점(entry point)
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

    /**
     * Avro 주문 이벤트 기반 5분 간격 주문 수 비교 통계 조회
     *
     * 최근 5분 vs 그 이전 5분의 주문 수를 비교하여
     * 변화량과 변화율을 반환한다.
     */
    @GetMapping("/avro/stats/count")
    fun getAvroOrderCountStats(): ResponseEntity<StatsResponse<OrderCountComparisonStats>> {
        val stats = avroOrderStreamsService.orderCountComparison()
        val response = StatsResponse(
            success = stats != null,
            data = stats,
            message = if (stats != null) {
                "Avro order count statistics retrieved successfully"
            } else {
                "Avro streams not available or no data found"
            }
        )
        return ResponseEntity.ok(response)
    }


}