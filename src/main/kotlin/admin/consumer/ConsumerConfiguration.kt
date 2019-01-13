package admin.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@EnableKafka
@Configuration
open class ConsumerConfiguration {

    @Autowired
    private val kafkaProperties: KafkaProperties? = null

    @Bean
    open fun ConsumerFactory(): ConsumerFactory<kotlin.Any,kotlin.Any> {
        val consumerProperties = mutableMapOf<String, kotlin.Any>()
        consumerProperties.apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties!!.bootstrapServers)
            put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties?.consumer!!.clientId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.consumer!!.keyDeserializer)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.consumer!!.valueDeserializer)
        }
        return DefaultKafkaConsumerFactory(consumerProperties)
    }

    @Bean
    open fun CreateTopicConsumerListenerFactory(): ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any>()
        factory.consumerFactory = ConsumerFactory()
        factory.setRecordFilterStrategy{
            it.value() == "create"
        }
        return factory
    }

    @Bean
    open fun DeleteTopicConsumerListenerFactory(): ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any>()
        factory.consumerFactory = ConsumerFactory()
        factory.setRecordFilterStrategy{
            it.value() == "delete"
        }
        return factory
    }

    @Bean
    open fun UpdateTopicConsumerListenerFactory(): ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<kotlin.Any,kotlin.Any>()
        factory.consumerFactory = ConsumerFactory()
        factory.setRecordFilterStrategy{
            it.value() == "update"
        }
        return factory
    }
}