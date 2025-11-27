package org.example.basic

import org.example.model.OrderEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import kotlin.math.log

//이벤트 전송 Publisher
@Component
class `OrderEventPublisher.kt`(
    private val kafkaTemplate: KafkaTemplate<String, OrderEvent>,//Kafka로 메시지를 보내는 기능을 제공하는 "전용 템플릿"
    //topic값도 환경변수로 잡기
    @Value("\${kafka.topics.orders}") private val ordersTopic: String
) {
    private val logger = LoggerFactory.getLogger(`OrderEventPublisher.kt`::class.java)
    //파티션의 저장공간을 정하기 위한 key(orderEvent.orderId)
    //같은 키는 같은 파티션에 저장
    fun publishOrderEvent(orderEvent: OrderEvent) {
        try {
            // hash(key) % 토픽의 파티션 수 -> 파티션에 적절하게 분산될것임
            kafkaTemplate.send(ordersTopic, orderEvent.orderId, orderEvent)

                .whenComplete { _, ex ->
                    if (ex != null) {
                        logger.error("Error when publishing order event", ex)
                    }else {
                        logger.info("Successfully published order event")
                    }
                }
        } catch (ex : Exception) {
            logger.error("Error Publishing order event", ex)
        }
    }
}