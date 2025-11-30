package org.example.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class AvroOrderEventProducer(
    private val avroKafkaTemplate: KafkaTemplate<String, GenericRecord>,//KafkaTemplate- KafkaConfig에서 구현한 bean 사용(avroKafkaTemplate)
    private val schemaManager: SchemaManager
) {
    private val logger = LoggerFactory.getLogger(AvroOrderEventProducer::class.java)

    fun publishOrderEvent(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal
    ) {
        try {
            val schema = schemaManager.orderEventSchema()
            val avroRecord = createAvroRecord(orderId, customerId, quantity, price, schema)

            avroKafkaTemplate.send("orders-avro", orderId, avroRecord)
                .whenComplete { result, ex ->
                    if (ex == null) {
                        logger.info("Avro order event published successfully: orderId={}, partition={}, offset={}",
                            orderId, result?.recordMetadata?.partition(), result?.recordMetadata?.offset())
                    } else {
                        logger.error("Failed to publish Avro order event: orderId={}", orderId, ex)
                    }
                }

        } catch (ex: Exception) {
            logger.error("Error creating Avro record for order: orderId={}", orderId, ex)
        }
    }


    private fun createAvroRecord(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal,
        schema: Schema
    ): GenericRecord {
        //record에 값 넣어주기
        val record = GenericData.Record(schema)

        val now = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()

        record.put("orderId", orderId)
        record.put("customerId", customerId)
        record.put("quantity", quantity)
        record.put("price", convertPriceToBytes(price))
        record.put("status", GenericData.EnumSymbol(schema.getField("status").schema(), "PENDING"))
        record.put("createdAt", now)
        record.put("updatedAt", now)
        record.put("version", 1L)

        return record
    }

    private fun convertPriceToBytes(price: BigDecimal): ByteBuffer {
        val scaled = price.setScale(2)
        val unscaledValue = scaled.unscaledValue()
        return ByteBuffer.wrap(unscaledValue.toByteArray())
    }
}