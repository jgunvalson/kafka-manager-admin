package admin.consumer.listener

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

@Service
class CreateTopicListener {

    private static final Logger log = LoggerFactory.getLogger(CreateTopicListener.class)

    @KafkaListener(
            topics = "__api_input_topic.v1",
            containerFactory = "createTopicKafkaListenerContainerFactory"
    )
    void consume(@Payload String m,
                 @Header MessageHeaders h) {

        log.info("Admin service has received a create-topic message, {}.", m)
        h.entrySet().each {
            log.info("Headers, {}.", it.key)
        }
    }
}
