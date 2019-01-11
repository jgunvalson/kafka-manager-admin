package admin.ops

import kafkamanager.model.TopicRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component

@Component
class CreateTopics() {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(CreateTopics::class.java)
    }

    @Autowired
    private val kafkaProperties: KafkaProperties? = null



    /*
    Maybe this should return an object that describes what was found
     */
    fun createTopicsFromTopic(vararg topicList: TopicRequest): Unit {

        val adminProperties = mutableMapOf<String, kotlin.Any>()
        adminProperties.apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties?.consumer!!.bootstrapServers)
            put(AdminClientConfig.RETRIES_CONFIG, "30")
        }
        val adminClient = AdminClient.create(adminProperties)

        val newTopics: List<NewTopic> = topicList.map {
            NewTopic(it.getTopicName(), it.getPartitions(), (it.getReplication().toShort()))
        }

        adminClient.createTopics(newTopics)
    }
}