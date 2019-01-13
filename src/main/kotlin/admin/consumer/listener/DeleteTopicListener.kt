package admin.consumer.listener

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload

class DeleteTopicListener {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(DeleteTopicListener::class.java)
    }

    @KafkaListener(
            topics = arrayOf<String>("__api_input_topic.v1"),
            containerFactory = "DeleteTopicKafkaListenerContainerFactory")

    fun consume(@Payload m: String,
                @Header h: MessageHeaders): Unit {

        log.info("Admin service has received a delete-topic message, {}.", m)
        h.forEach{
            log.info("Headers, {}.", it.key)
        }
    }
}
