package org.example.avro

import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class AvroOrderEventConsumer {

    private val logger = LoggerFactory.getLogger(AvroOrderEventConsumer::class.java)

    @KafkaListener(
        topics = ["orders-avro"],
        groupId = "avro-order-processor-v1",
        containerFactory = "avroKafkaListenerContainerFactory"
    )
    fun handleOrderEventV1(
        @Payload avroRecord: GenericRecord, //avro에 따라 동적으로 다룰수 있음
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        try {
            val orderData = extractOrderDataFromAvro(avroRecord)

            logger.info("Processing Avro order: orderId={}, partition={}, offset={}",
                orderData.orderId, partition, offset)
        } catch (e: Exception) {
            logger.error(e.message, e)
        }
    }


    //get을 통해서 원하는 값 추출
    private fun extractOrderDataFromAvro(record: GenericRecord): OrderDataV1 {
        return OrderDataV1(
            orderId = record.get("orderId").toString(),
            customerId = record.get("customerId").toString(),
            quantity = record.get("quantity") as Int,
            price = convertBytesToPrice(record.get("price") as ByteBuffer),
            timestamp = convertTimestamp(record.get("createdAt") as Long),
            status = record.get("status").toString(),
            version = record.get("version") as Long
        )
    }

    //가격을 변환해주는 함수
    private fun convertBytesToPrice(byteBuffer: ByteBuffer): BigDecimal {
        val bytes = ByteArray(byteBuffer.remaining())
        byteBuffer.get(bytes)
        val bigInt = BigInteger(bytes)
        return BigDecimal(bigInt, 2)
    }

    //시간을 변환해주는 함수
    private fun convertTimestamp(epochMilli: Long): LocalDateTime {
        // unix epoch milliseconds
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
    }
}

//order-entity.avsc 파일에 맞추어서 class 선언
data class OrderDataV1(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val timestamp: LocalDateTime,
    val status: String,
    val version: Long
)