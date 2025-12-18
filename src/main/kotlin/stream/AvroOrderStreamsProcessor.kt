package org.example.stream

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.WindowStore
import org.example.config.KafkaConfig
import org.example.model.OrderEvent
import org.example.model.WindowedOrderCount
import org.example.model.WindowedSalesData
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Avro 주문 이벤트를 Kafka Streams로 처리해서
 *
 * - 10초 윈도우 기준 주문 수 집계(`order-count-store-avro`)
 * - 1시간 윈도우 기준 매출 통계 집계(`sales-stats-store-avro`)
 *
 * 를 생성한다.
 */
@Component
class AvroOrderStreamsProcessor(
    // Avro 주문 이벤트가 들어오는 토픽
    @Value("\${kafka.topics.orders-avro:orders-avro}")
    private val ordersAvroTopic: String,
) {

    private val logger = LoggerFactory.getLogger(AvroOrderStreamsProcessor::class.java)

    private val orderEventSerde = createJsonSerde<OrderEvent>()
    private val windowedOrderCountSerde = createJsonSerde<WindowedOrderCount>()
    private val windowedSalesDataSerde = createJsonSerde<WindowedSalesData>()

    private inline fun <reified T> createJsonSerde(): JsonSerde<T> {
        return JsonSerde<T>().apply {
            configure(
                mapOf(
                    "spring.json.trusted.packages" to "org.example.model",
                    "spring.json.add.type.headers" to false,
                    "spring.json.value.default.type" to T::class.java.name
                ),
                false
            )
        }
    }

    /**
     * Avro 토픽을 읽어서 내부적으로 `OrderEvent` 로 변환한 뒤
     * JSON 기반 스트림과 동일한 윈도우 집계를 수행하는 서브-토폴로지를
     * 전달받은 `StreamsBuilder` 에 등록한다.
     *
     * 주의: 여기서는 `builder.build()` 를 호출하지 않는다.
     *       최종 Topology 빌드는 하나의 @Bean (예: OrderStreamsProcessor.orderProcessingTopology) 에서만 수행해야 한다.
     */
    fun registerAvroTopology(builder: StreamsBuilder) {
        val avroSerde: Serde<GenericRecord> = GenericAvroSerde().apply {
            configure(
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to KafkaConfig.SCHEMA_REGISTRY_URL
                ),
                false // value serde
            )
        }

        // Avro 주문 이벤트 스트림
        val avroOrderStream: KStream<String, GenericRecord> =
            builder.stream(ordersAvroTopic, Consumed.with(Serdes.String(), avroSerde))

        // Avro(GenericRecord) -> 도메인 모델(OrderEvent) 변환
        // 1) key 를 명시적으로 orderId 로 설정
        // 2) value 를 OrderEvent 로 변환
        val orderEventStream: KStream<String, OrderEvent> =
            avroOrderStream
                .selectKey { _, record -> record.get("orderId").toString() }
                .mapValues { record -> toOrderEvent(record) }

        // 기존 JSON 파이프라인과 동일한 집계 로직 재사용
        orderCountStatsStream(orderEventStream)
        salesStatsStream(orderEventStream)
    }

    // --------------------------
    // 윈도우 집계 로직 (기존과 동일)
    // --------------------------

    private fun orderCountStatsStream(orderStream: KStream<String, OrderEvent>) {
        orderStream
            .groupByKey(Grouped.with(Serdes.String(), orderEventSerde))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(
                { WindowedOrderCount() },
                { _, _, aggregate -> aggregate.increment() },
                Materialized.`as`<String, WindowedOrderCount, WindowStore<Bytes, ByteArray>>("order-count-store-avro")
                    .withValueSerde(windowedOrderCountSerde)
            )
    }

    private fun salesStatsStream(orderStream: KStream<String, OrderEvent>) {
        orderStream
            .groupBy(
                { _, orderEvent -> orderEvent.customerId },
                Grouped.with(Serdes.String(), orderEventSerde)
            )
            .windowedBy(TimeWindows.of(Duration.ofHours(1)))
            .aggregate(
                { WindowedSalesData() },
                { _, orderEvent, aggregate -> aggregate.add(orderEvent.price) },
                Materialized.`as`<String, WindowedSalesData, WindowStore<Bytes, ByteArray>>("sales-stats-store-avro")
                    .withValueSerde(windowedSalesDataSerde)
            )
    }

    // --------------------------
    // Avro(GenericRecord) -> OrderEvent 변환 유틸
    // (AvroOrderEventConsumer 와 동일한 스키마를 사용한다고 가정)
    // --------------------------

    private fun toOrderEvent(record: GenericRecord): OrderEvent {
        val orderId = record.get("orderId").toString()
        val customerId = record.get("customerId").toString()
        val quantity = record.get("quantity") as Int
        val price = convertBytesToPrice(record.get("price") as ByteBuffer)
        val createdAtEpochMillis = record.get("createdAt") as Long
        val createdAt = convertTimestamp(createdAtEpochMillis)

        return OrderEvent(
            orderId = orderId,
            customerId = customerId,
            quantity = quantity,
            price = price,
            status = record.get("status").toString(),
            timestamp = createdAt
        )
    }

    private fun convertBytesToPrice(byteBuffer: ByteBuffer): BigDecimal {
        val bytes = ByteArray(byteBuffer.remaining())
        byteBuffer.get(bytes)
        val bigInt = BigInteger(bytes)
        return BigDecimal(bigInt, 2)
    }

    private fun convertTimestamp(epochMilli: Long): LocalDateTime {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
    }
}


