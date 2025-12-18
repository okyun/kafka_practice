package org.example.controller


import org.example.model.*
import org.example.stream.OrderStreamsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*


//✔ StatsController = “API로 데이터 전달하는 곳”
//
//StatsController 역할:
//
//HTTP 요청(GET /api/stats/orders/count) 받음
//
//OrderStreamsService 에 요청해서 데이터 가져옴
//
//JSON 형태로 응답
//
//즉,
//
//"외부에서 데이터를 가져갈 수 있게 서버의 창구(REST API)" 역할

@RestController
@RequestMapping("/api/stats/orders")
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