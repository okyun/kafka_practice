package org.example.basic

import org.example.model.OrderEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
//KafkaTemplate → 보내는 쪽 (Producer)
//KafkaListener → 받는 쪽 (Consumer),-topics,groupId 설정가능
@Component
class OrderEventConsumer {
    private val logger = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-processing-group",
        concurrency = "3",//병렬처리 가능한 쓰레드 갯수, 파티션의 갯수가 일반적인 쓰레드 갯수
        containerFactory = "orderEventKafkaListenerContainerFactory"
    )//같은 토픽을 공유해도, groupId는 다르게 해서 “서로 다른 기능/역할”을 담당하게 할수 있음(주문알림, 배송시작.. 이런일들을 각각 처리 가능하게 함)
    fun processOrder(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: String,
        @Header(KafkaHeaders.OFFSET) offset: Long,
    ) {
        logger.info("Received order event: {}", orderEvent)
        try {
            processLogic()

            logger.info("Received order event: {}", orderEvent)
        } catch (ex : Exception)  {
            logger.error(ex.message, ex)
        }
    }

    private fun processLogic() {
        Thread.sleep(100)
    }
}


@Component
class OrderAnalyticsConsumer {

    private val logger = LoggerFactory.getLogger(OrderAnalyticsConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-analytics-group",
        concurrency = "2",
        containerFactory = "orderEventKafkaListenerContainerFactory"
    )
    fun collectAnalytics(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        logger.info("Collecting analytics for order {} from partition {}",
            orderEvent.orderId, partition)

        try {
            updateCustomerStatistics(orderEvent)

        } catch (ex: Exception) {
            logger.error("Failed to collect analytics for order {}: {}",
                orderEvent.orderId, ex.message)
        }
    }

    private fun updateCustomerStatistics(orderEvent: OrderEvent) {
        logger.debug("Updated customer statistics for {}", orderEvent.customerId)
    }

}

@Component
class OrderNotificationConsumer {

    private val logger = LoggerFactory.getLogger(OrderNotificationConsumer::class.java)

    @KafkaListener(
        topics = ["\${kafka.topics.orders}"],
        groupId = "order-notification-group",
        concurrency = "1",
        containerFactory = "orderEventKafkaListenerContainerFactory"
    )
    //partition - 메시지가 어느 Partition에서 왔는지 알아야 하는 상황
    fun sendNotifications(
        @Payload orderEvent: OrderEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        logger.info("Sending notifications for order {} from partition {}",
            orderEvent.orderId, partition)

        try {
            if (isHighValueOrder(orderEvent)) {
                sendHighValueOrderSms(orderEvent)
            }

        } catch (ex: Exception) {
            logger.error("Failed to send notifications for order {}: {}",
                orderEvent.orderId, ex.message)
        }
    }

    private fun sendHighValueOrderSms(orderEvent: OrderEvent) {
        logger.info("SMS sent for high-value order {}", orderEvent.orderId)
    }

    private fun isHighValueOrder(orderEvent: OrderEvent): Boolean {
        return orderEvent.price.compareTo(java.math.BigDecimal("1000")) >= 0
    }
}