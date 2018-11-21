package admin.Domain

import org.springframework.boot.autoconfigure.kafka.KafkaProperties

class Config {

    @KafkaProperties
    KafkaProperties kafkaProperties
}
