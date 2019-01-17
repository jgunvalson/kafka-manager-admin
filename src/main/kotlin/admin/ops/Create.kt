package admin.ops

import admin.ops.converter.AclRequestConverter
import admin.ops.converter.UpdateTopicConfigurationConverter
import avro.shaded.com.google.common.collect.ImmutableMap
import kafkamanager.model.AclRequest
import kafkamanager.model.NewTopicRequest
import kafkamanager.model.UpdateTopicConfigurationRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component

@Component
class Create() {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(Create::class.java)
    }

    @Autowired
    private val kafkaProperties: KafkaProperties? = null

    @Autowired
    private val aclRequestConverter = AclRequestConverter()

    @Autowired
    private val updateTopicConfigurationConverter = UpdateTopicConfigurationConverter()

    /*
    Maybe this should return an object that describes what was found
     */
    fun createTopicsFromRequest(vararg topicList: NewTopicRequest): Unit {

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


    fun createAclFromRequest(vararg aclList: AclRequest): Unit {
        val adminProperties = mutableMapOf<String, Any>()
        adminProperties.apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties?.consumer!!.bootstrapServers)
            put(AdminClientConfig.RETRIES_CONFIG, "30")
        }


        val adminClient = AdminClient.create(adminProperties)


        val newAcls: List<AclBinding> = aclList.map {
            aclRequestConverter.convertAclRequest(it)
        }


        adminClient.createAcls(newAcls)
    }


    fun updateTopicConfigurationFromRequest(vararg updateTopicConfigurationRequest: UpdateTopicConfigurationRequest): Unit {
        val adminProperties = mutableMapOf<String, Any>()
        adminProperties.apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties?.consumer!!.bootstrapServers)
            put(AdminClientConfig.RETRIES_CONFIG, "30")
        }


        val adminClient = AdminClient.create(adminProperties)


        val newConfigurations: MutableMap<ConfigResource, Config> = mutableMapOf()


        for (request : UpdateTopicConfigurationRequest in updateTopicConfigurationRequest) {
            val updateRequestPair = updateTopicConfigurationConverter.convertTopicConfiguration(request)
            newConfigurations.put(updateRequestPair.first, updateRequestPair.second)
        }


        adminClient.alterConfigs(newConfigurations)
    }
}