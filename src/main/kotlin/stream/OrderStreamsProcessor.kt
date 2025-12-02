package org.example.stream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.WindowStore
import org.example.model.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Duration

//
//✔ OrderStreamsProcessor = “값을 저장하는 곳(Producer → 처리 → StateStore)”
//
//Kafka Topic에서 주문 이벤트(OrderEvent)를 실시간으로 읽어옴
//
//필터링 / 매핑 / 윈도우 집계 수행
//
//RocksDB 기반 State Store(예: order-count-store)에 저장
//
//일부는 다른 Kafka Topic에도 전송함 (fraud-alerts, high-value-orders)
//“실시간 데이터 처리 + 상태(State) 저장 담당”
//시스템의 두뇌 + 데이터 생산 파트
@Component
class OrderStreamsProcessor(
    // 주문 이벤트가 들어오는 원본 토픽
    @Value("\${kafka.topics.orders}") private val ordersTopic: String,
    // 고액 주문만 따로 보내는 토픽
    @Value("\${kafka.topics.high-value-orders}") private val highValueOrdersTopic: String,
    // 사기 의심 이벤트를 보내는 토픽
    @Value("\${kafka.topics.fraud-alerts}") private val fraudAlertsTopic: String,
) {
    private val logger = LoggerFactory.getLogger(OrderStreamsProcessor::class.java)

    private val orderEventSerde = createJsonSerde<OrderEvent>()
    private val fraudAlertSerde = createJsonSerde<FraudAlert>()
    private val windowedOrderCountSerde = createJsonSerde<WindowedOrderCount>()
    private val windowedSalesDataSerde = createJsonSerde<WindowedSalesData>()

    //JsonSerde - kafka stream에서만 사용되는 직렬화, 역직렬화의 줄인말
    private inline fun <reified T> createJsonSerde() : JsonSerde<T> {
        return JsonSerde<T>().apply {
            configure(mapOf(
                "spring.json.trusted.packages" to "org.example.model",
                "spring.json.add.type.headers" to false,
                "spring.json.value.default.type" to T::class.java.name
            ), false)
        }
    }

//    애플리케이션이 시작될 때(Spring이 빈 만들 때) 한 번만 실행되고,
//    그 결과로 만들어진 토폴로지를 기반으로
//    KafkaStreams가 계속 메시지를 감시/처리하는 구조
    @Bean
    fun orderProcessingTopology(builder : StreamsBuilder) : Topology   {
        val orderStream : KStream<String, OrderEvent> = builder.stream(ordersTopic, Consumed.with(Serdes.String(), orderEventSerde))

        //OrderEventPublisher 에서 kafkaTemplate.send(ordersTopic, orderEvent.orderId, orderEvent) key를 orderId로 잡음
        highValueStream(orderStream)
        fraudStream(orderStream)
        orderCountStatsStream(orderStream)
        salesStatsStream(orderStream)

        return builder.build()
    }

    private fun highValueStream(orderStream :  KStream<String, OrderEvent>) {
        val highValueStream = orderStream.filter { _, orderEvent ->
            logger.info("Filtering high Value Stream order: {}", orderEvent.orderId)
            orderEvent.price >= BigDecimal("1000")
        }

        highValueStream.to(highValueOrdersTopic, Produced.with(Serdes.String(), orderEventSerde))
    }
    //사기 탐지 (필터 사용)
    private fun fraudStream(orderStream :  KStream<String, OrderEvent>) {
        val fraudStream = orderStream.filter { _, orderEvent ->
            orderEvent.price >= BigDecimal("5000") ||
                    orderEvent.quantity > 100 ||
                    orderEvent.price.multiply(BigDecimal.valueOf(orderEvent.quantity.toLong())) >= BigDecimal("10000")
        }.mapValues { orderEvent ->
            val reason = when {
                orderEvent.price >= BigDecimal("5000") -> "High single order value"
                orderEvent.quantity > 100 -> "High quantity order"
                else -> "High total order value"
            }

            val severity = when {
                orderEvent.price >= BigDecimal("10000") -> FraudSeverity.CRITICAL
                orderEvent.price >= BigDecimal("5000") -> FraudSeverity.HIGH
                orderEvent.quantity > 100 -> FraudSeverity.MEDIUM
                else -> FraudSeverity.LOW
            }

            FraudAlert(
                orderId = orderEvent.orderId,
                customerId = orderEvent.customerId,
                reason = reason,
                severity = severity,
            )
        }

        fraudStream.to(fraudAlertsTopic, Produced.with(Serdes.String(), fraudAlertSerde))
    }

    //일정시간 구간으로 나누어서 구간별로 집계한다.10초마다 어떤 고객이 활발할지, 10 초 마다 어떤 주문이 급증했는지 확인 가능
    private fun orderCountStatsStream(orderStream: KStream<String, OrderEvent>) {
        orderStream
            .groupByKey(Grouped.with(Serdes.String(), orderEventSerde))
            //groupByKey는 key인 orderId로 지정되어 잇다.
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))//10초 단위값으로 구간으로 집계
            .aggregate(
                { WindowedOrderCount() },
                { _, _, aggregate -> aggregate.increment() },
                Materialized.`as`<String, WindowedOrderCount, WindowStore<Bytes, ByteArray>>("order-count-store")  // ⭐ 여기서 RocksDB 생성 & 저장
                    .withValueSerde(windowedOrderCountSerde)
            )
    }

    // ⚠️ 이 스트림도 따로 to()로 내보내지 않고 상태 저장소에만 집계 결과를 유지
    // → 나중에 API 레이어에서 이 상태 스토어를 조회해서 "실시간 매출 통계"로 활용 가능
    private fun salesStatsStream(orderStream: KStream<String, OrderEvent>) {
        /*
            <"customer1", OrderEvent(orderId="order1", customerId="customer1", price=100)>
            <"customer2", OrderEvent(orderId="order2", customerId="customer2", price=200)>
            <"customer1", OrderEvent(orderId="order3", customerId="customer1", price=150)>


            customer1: [OrderEvent(order1, 100), OrderEvent(order3, 150)]
            customer2: [OrderEvent(order2, 200)]
        */
        orderStream
            .groupBy(
                { key, orderEvent -> orderEvent.customerId },
                Grouped.with(Serdes.String(), orderEventSerde)
            )
            .windowedBy(TimeWindows.of(Duration.ofHours(1)))
            .aggregate(
                { WindowedSalesData() },
                { _, orderEvent, aggregate -> aggregate.add(orderEvent.price) },
                Materialized.`as`<String, WindowedSalesData, WindowStore<Bytes, ByteArray>>("sales-stats-store")
                    .withValueSerde(windowedSalesDataSerde)
            )
    }
}
//
//| 항목        | orderCountStatsStream     | salesStatsStream                          |
//| --------- | ------------------------- | ----------------------------------------- |
//| 집계 기준 Key | orderId (기본 key 그대로)      | customerId (key 재지정)                      |
//| 집계 목적     | 전체 주문 수 (주문량 변화 감지)       | 고객별 매출/주문 횟수                              |
//| 윈도우       | 10초                       | 1시간                                       |
//| 상태 저장소    | order-count-store         | sales-stats-store                         |
//| 상태 값      | WindowedOrderCount(count) | WindowedSalesData(totalSales, orderCount) |
//| Key 형태    | windowed(orderId)         | windowed(customerId)                      |
