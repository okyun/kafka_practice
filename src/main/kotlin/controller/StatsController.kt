package org.example.controller


import org.example.model.*
import org.example.stream.OrderStreamsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/stats")
class StatsController(
    private val orderStreamsService: OrderStreamsService
) {

    @GetMapping("/orders/count")
    fun getOrderCountStats(): ResponseEntity<StatsResponse<OrderCountComparisonStats>> {
        //시간대 별로 주문량 비교가능
        val stats = orderStreamsService.orderCountComparison()
        val response = StatsResponse(
            success = true,
            data = stats,
            message = "Order count statistics retrieved successfully"
        )
        return ResponseEntity.ok(response)
    }
}