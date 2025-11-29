package org.example.stream

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreType
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
import kotlin.math.log

@Service
class OrderStreamsService(
    private val factory : StreamsBuilderFactoryBean // 카프카 인스턴스를 관리하는 빈이고,카프카 스트림의 윈도우 저장소를 이용해서 시간 기반의 데이터를 조회 하는데 사용된다.
) {

    private val logger = LoggerFactory.getLogger(OrderStreamsService::class.java)


    fun orderCountComparison() : OrderCountComparisonStats? {
        return try {
            val stream = factory.kafkaStreams
            if (stream == null ||  stream.state() != KafkaStreams.State.RUNNING) {
                return null
            }

            val store : ReadOnlyWindowStore<String, WindowedOrderCount> = stream.store(
                StoreQueryParameters.fromNameAndType("order-count-store", QueryableStoreTypes.windowStore())
            )// ← RocksDB 읽기 준비

            val now = Instant.now()

            /*
                   9시
                   8시 55분 ~ 9시 까지의 데이터
                   8시 50분 ~ 8시 55분 까지의 데이터
             */

            val currentPeriodEnd = now
            val currentPeriodStart = now.minusSeconds(300) // 5분전

            val prevPeriodEnd = currentPeriodStart
            val prevPeriodStart = currentPeriodStart.minusSeconds(300)

            val currentCount = countForPeriod(store, currentPeriodStart, currentPeriodEnd) //  8시 55분 ~ 9시 까지의 (주문)데이터
            val previousCount = countForPeriod(store, prevPeriodStart, prevPeriodEnd) // 8시 50분 ~ 8시 55분 까지의 (주문)데이터

            val changeCount = currentCount - previousCount
            val changePercentage = if (previousCount > 0) {
                (changeCount.toDouble() / previousCount.toDouble()) * 100.0
            } else if (currentCount > 0) {
                100.0
            } else {
                0.0
            }
            //어떤 시간대가 더 많은지 백분율로 계산

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
        } catch (e : Exception) {
            logger.error("Failed to get streams info", e.message)
            return null
        }
    }
    //이 시간대에 어떤 주문이 더 많이 들어왔는지 확인하는 메소드
    private fun countForPeriod(
        store : ReadOnlyWindowStore<String, WindowedOrderCount>,
        startTime : Instant,
        endTime : Instant
    ) : Long {
        var totalCount =0L

        store.fetchAll(startTime, endTime).use { iter -> // ← 실제 RocksDB에서 데이터 읽기
            while (iter.hasNext()) {
                val entry = iter.next()
                totalCount += entry.value.count
            }
        }

        return totalCount
    }
}