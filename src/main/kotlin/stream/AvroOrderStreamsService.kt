package org.example.stream

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.example.model.OrderCountComparisonStats
import org.example.model.PeriodStats
import org.example.model.WindowedOrderCount
import org.slf4j.LoggerFactory
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Avro 주문 이벤트(`orders-avro`) 기반으로 생성된 상태 저장소를 조회하는 서비스.
 *
 * 상태 저장소 이름은 JSON 기반 스트림과 공유(`order-count-store`)하므로,
 * Avro / JSON 어느 쪽에서 들어온 주문이든 모두 합쳐서 5분 단위 비교 통계를 조회할 수 있다.
 */
@Service
class AvroOrderStreamsService(
    private val factory: StreamsBuilderFactoryBean
) {

    private val logger = LoggerFactory.getLogger(AvroOrderStreamsService::class.java)

    /**
     * 최근 5분 vs 그 이전 5분의 주문 수를 비교한다.
     *
     * - 현재 기간: now - 5분 ~ now
     * - 이전 기간: now - 10분 ~ now - 5분
     */
    fun orderCountComparison(): OrderCountComparisonStats? {
        return try {
            val streams = factory.kafkaStreams
            if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
                return null
            }

            val store: ReadOnlyWindowStore<String, WindowedOrderCount> = streams.store(
                StoreQueryParameters.fromNameAndType(
                    "order-count-store",
                    QueryableStoreTypes.windowStore<String, WindowedOrderCount>()
                )
            )

            val now = Instant.now()

            val currentPeriodEnd = now
            val currentPeriodStart = now.minusSeconds(300) // 최근 5분

            val prevPeriodEnd = currentPeriodStart
            val prevPeriodStart = currentPeriodStart.minusSeconds(300) // 그 이전 5분

            val currentCount = countForPeriod(store, currentPeriodStart, currentPeriodEnd)
            val previousCount = countForPeriod(store, prevPeriodStart, prevPeriodEnd)

            val changeCount = currentCount - previousCount
            val changePercentage = when {
                previousCount > 0 ->
                    (changeCount.toDouble() / previousCount.toDouble()) * 100.0

                currentCount > 0 -> 100.0
                else -> 0.0
            }

            OrderCountComparisonStats(
                currentPeriod = PeriodStats(
                    windowStart = LocalDateTime.ofInstant(currentPeriodStart, ZoneOffset.UTC),
                    windowEnd = LocalDateTime.ofInstant(currentPeriodEnd, ZoneOffset.UTC),
                    orderCount = currentCount
                ),
                previousPeriod = PeriodStats(
                    windowStart = LocalDateTime.ofInstant(prevPeriodStart, ZoneOffset.UTC),
                    windowEnd = LocalDateTime.ofInstant(prevPeriodEnd, ZoneOffset.UTC),
                    orderCount = previousCount
                ),
                changeCount = changeCount,
                changePercentage = changePercentage,
                isIncreasing = changeCount > 0
            )
        } catch (e: Exception) {
            logger.error("Failed to get avro streams info", e)
            null
        }
    }

    private fun countForPeriod(
        store: ReadOnlyWindowStore<String, WindowedOrderCount>,
        startTime: Instant,
        endTime: Instant
    ): Long {
        var totalCount = 0L

        store.fetchAll(startTime, endTime).use { iter ->
            while (iter.hasNext()) {
                val entry = iter.next()
                totalCount += entry.value.count
            }
        }

        return totalCount
    }
}


