package consumer

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
class ConsumerConfiguration {

    @Autowired
    KafkaProperties kafkaProperties

    @Bean
    ConsumerFactory<String,String> consumerFactory() {
        Map<String, Object> consumerProperties = new Properties()
//        May not want to have a default bootstrap server for the default consumer factory
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.consumer.bootstrapServers)
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                kafkaProperties.consumer.clientId)
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                kafkaProperties.consumer.keyDeserializer)
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                kafkaProperties.consumer.valueDeserializer)
        return new DefaultKafkaConsumerFactory<>(consumerProperties)
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String,String> createTopicKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String,String> factory = new ConcurrentKafkaListenerContainerFactory<>()
        factory.setConsumerFactory(consumerFactory())
//        I have no idea how to actually read the value of a record or what I'm doing, YOLO
        factory.setRecordFilterStrategy({
            r -> r.value().toLowerCase().contains("create")
        })
        return factory
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String,String> deleteTopicKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>()
        factory.setConsumerFactory(consumerFactory())
//        I have no idea how to actually read the value of a record or what I'm doing, YOLO
        factory.setRecordFilterStrategy({
            r -> r.value().toLowerCase().contains("delete")
        })
        return factory
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String,String> updateTopicKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>()
        factory.setConsumerFactory(consumerFactory())
//        I have no idea how to actually read the value of a record or what I'm doing, YOLO
        factory.setRecordFilterStrategy({
            r -> r.value().toLowerCase().contains("update")
        })
        return factory
    }

}
