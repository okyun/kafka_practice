package org.example.config

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.example.model.OrderEvent
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaConfig(

) {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    companion object {
        const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
    }


    //// ----------- listener --------------
    @Bean
    fun orderEventConsumerFactory(): ConsumerFactory<String, OrderEvent> {
        //key, value에 직렬화 설정방식
        val props = mapOf(
            //ErrorHandlingDeserializer- 역직렬화 실패시 어플리케이션 종료 , 잘못된 json 방식 처리

            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to StringDeserializer::class.java,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to JsonDeserializer::class.java,
            // 메시지 포맷이 잘못되어도 consumer 중단 하지 않는 형태
            JsonDeserializer.VALUE_DEFAULT_TYPE to OrderEvent::class.java,
            JsonDeserializer.TRUSTED_PACKAGES to "*",//전체 path 허용
            JsonDeserializer.USE_TYPE_INFO_HEADERS to false
        )

        return DefaultKafkaConsumerFactory(props)
    }

    //kafkalistener의 어노테이션의 수신을 위한 bean
    @Bean
    fun orderEventKafkaListenerContainerFactory() : ConcurrentKafkaListenerContainerFactory<String, OrderEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, OrderEvent>()
        factory.consumerFactory = orderEventConsumerFactory()
        return factory
    }


    //OrderEvent이 아니 다른 타입도 메시지로 받기 위한 메소드
    @Bean
    fun genericConsumerFactory(): ConsumerFactory<String, Any> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to StringDeserializer::class.java,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to JsonDeserializer::class.java,
            JsonDeserializer.TRUSTED_PACKAGES to "*",
            JsonDeserializer.USE_TYPE_INFO_HEADERS to true
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun genericKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = genericConsumerFactory()
        return factory
    }

    @Bean
    fun cdcConsumerFactory(): ConsumerFactory<String, String> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,//server_id
            ConsumerConfig.GROUP_ID_CONFIG to "real-order-cdc-processor",//group_id
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to 1000
        )
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun cdcKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = cdcConsumerFactory()
        return factory
    }

    @Bean
    fun avroConsumerFactory(): ConsumerFactory<String, GenericRecord> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to false,
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
        )

        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun avroKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, GenericRecord> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, GenericRecord>()
        factory.consumerFactory = avroConsumerFactory()
        return factory
    }

    //// ----------- producer --------------


    @Bean
    fun orderEventProducerFactory(): ProducerFactory<String, OrderEvent> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            //직렬화, 역직렬화
            JsonSerializer.ADD_TYPE_INFO_HEADERS to true
        )
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, OrderEvent> {
        return KafkaTemplate(orderEventProducerFactory())
    }
    // Apache Avro = JSON처럼 생겼지만 훨씬 빠르고, 가볍고,
    // Schema(스키마)를 함께 사용해서 안전하게 직렬화/역직렬화하는 데이터 포맷.
    @Bean
    fun avroProducerFactory(): ProducerFactory<String, GenericRecord> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL,
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
            ProducerConfig.ACKS_CONFIG to "1", // "0", "1", "all", 0-확인안함, 1-리더한테 전달되었는지 확인,all - 복제본까지 확인(금융,결제 시스템에서 주로 사용 시간이 제일 올래걸림)
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384,
            ProducerConfig.LINGER_MS_CONFIG to 10,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "snappy", //압축 알고름즘- none, lz4, gzip, zstd
        )

        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun avroKafkaTemplate(): KafkaTemplate<String, GenericRecord> {
        return KafkaTemplate(avroProducerFactory())
    }

}