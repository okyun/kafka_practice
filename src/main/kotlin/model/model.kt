package org.example.model

import com.fasterxml.jackson.annotation.JsonFormat
import java.math.BigDecimal
import java.time.LocalDateTime

data class OrderEvent(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val eventType: String = "ORDER_CREATED",
    val status: String = "PENDING",
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    val timestamp: LocalDateTime = LocalDateTime.now()
)

data class CreateOrderRequest(
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal
)


// ----- stream -----
// 여기서부터는 "Kafka Streams로 분석/집계할 때" 사용하는 모델들

// 현재 기간 vs 이전 기간의 주문 수를 비교할 때 사용하는 응답 데이터
data class OrderCountComparisonStats(
    val currentPeriod: PeriodStats,   // 현재 기간의 통계
    val previousPeriod: PeriodStats,  // 이전 기간의 통계
    val changeCount: Long,            // 주문 수 변화량 (현재 - 이전)
    val changePercentage: Double,     // 변화율 (%)
    val isIncreasing: Boolean,        // 증가 중인지 여부 (true: 증가, false: 감소/동일)
)

// 특정 기간(window) 하나에 대한 통계 정보
data class PeriodStats(
    val windowStart: LocalDateTime,   // 윈도우 시작 시간
    val windowEnd: LocalDateTime,     // 윈도우 종료 시간
    val orderCount: Long              // 해당 기간의 주문 수
)

// 공통 응답 형식 (성공 여부 + 데이터 + 메시지)
data class StatsResponse<T>(
    val success: Boolean,             // 성공 여부
    val data: T?,                     // 실제 응답 데이터 (제네릭)
    val message: String,              // 설명 메시지
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    val timestamp: LocalDateTime = LocalDateTime.now() // 응답 생성 시간
)

// 윈도우(예: 최근 1분, 최근 5분 등) 안에서의 "주문 개수"를 표현하는 모델
data class WindowedOrderCount(
    val count: Long = 0L              // 현재까지 집계된 주문 수
) {
    // 주문이 하나 들어올 때마다 count를 1 증가시키는 편의 메서드
    fun increment(): WindowedOrderCount = WindowedOrderCount(count + 1)
}

// 윈도우 안에서의 "매출 통계"를 표현하는 모델
data class WindowedSalesData(
    val totalSales: BigDecimal = BigDecimal.ZERO, // 해당 기간 총 매출 금액
    val orderCount: Long = 0L                     // 해당 기간 주문 수
) {
    // 새로운 주문 금액을 더하면서, 주문 수를 1 증가시키는 메서드
    fun add(orderValue: BigDecimal): WindowedSalesData = WindowedSalesData(
        totalSales = totalSales.add(orderValue),  // 기존 매출 + 새 주문 금액
        orderCount = orderCount + 1               // 주문 수 + 1
    )
}

// 이상 거래(사기 의심)를 탐지했을 때, 알림 이벤트로 사용하는 모델
data class FraudAlert(
    val orderId: String,              // 의심 주문 ID
    val customerId: String,           // 고객 ID
    val reason: String,               // 사기 의심 사유 (예: "단시간 다수 결제")
    val severity: FraudSeverity,      // 심각도 (LOW ~ CRITICAL)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    val timestamp: LocalDateTime = LocalDateTime.now() // 알림 생성 시간
)

// 사기 의심 알림의 심각도 단계
enum class FraudSeverity {
    LOW,        // 낮은 위험
    MEDIUM,     // 중간 위험
    HIGH,       // 높은 위험
    CRITICAL    // 매우 위험
}