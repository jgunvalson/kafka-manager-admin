package admin.consumer.listener

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload

class UpdateTopicListener {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UpdateTopicListener::class.java)
    }

    @KafkaListener(
            topics = arrayOf<String>("__api_input_topic.v1"),
            containerFactory = "UpdateTopicKafkaListenerContainerFactory")

    fun consume(@Payload m: String,
                @Header h: MessageHeaders): Unit {

        log.info("Admin service has received a update-topic message, {}.", m)
        h.forEach{
            log.info("Headers, {}.", it.key)
        }
    }
}