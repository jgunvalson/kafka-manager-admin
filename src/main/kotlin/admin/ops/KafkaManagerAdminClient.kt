package admin.ops

import admin.config.clusters.AdminClusters
import admin.ops.converter.AclRequestConverter
import admin.ops.converter.DeleteResourceAclConverter
import admin.ops.converter.UpdateTopicConfigurationConverter
import admin.ops.converter.UpdateTopicPartitionsConverter
import kafkamanager.model.*
import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.stereotype.Component

@Component
@EnableKafka
class KafkaManagerAdminClient constructor(@Autowired private val adminClusters: AdminClusters,
                                          @Autowired private val aclRequestConverter: AclRequestConverter,
                                          @Autowired private val updateTopicConfigurationConverter: UpdateTopicConfigurationConverter,
                                          @Autowired private val updateTopicPartitionsConverter: UpdateTopicPartitionsConverter,
                                          @Autowired private val deleteResourceAclConverter: DeleteResourceAclConverter) {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(KafkaManagerAdminClient::class.java)
    }


    fun createTopicsFromRequest(newTopicRequest: NewTopicRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(newTopicRequest.getEnvironment(), null)


        val newTopics: List<NewTopic> = newTopicRequest.getNewTopics().map {
            NewTopic(it.getTopicName(), it.getPartitions(), (it.getReplication().toShort()))
        }


        adminClient?.createTopics(newTopics) ?: log.error("Failed to create new acls on request {}:{} because admin-client value was invalid or null.",
                newTopicRequest.getEnvironment(),
                newTopicRequest.getNewTopics().size)
    }


    fun createAclFromRequest(newAclRequest: NewAclRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(newAclRequest.getEnvironment(), null)


        val newAcls: List<AclBinding> = newAclRequest.getNewAcls().map {
            aclRequestConverter.convertAclRequest(it)
        }


        adminClient?.createAcls(newAcls) ?: log.error("Failed to create new acls on request {}:{} because admin-client value was invalid or null.",
                newAclRequest.getEnvironment(),
                newAclRequest.getNewAcls().size)
    }


    fun updateTopicConfigurationFromRequest(updateTopicConfigurationRequest: UpdateTopicConfigurationRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(updateTopicConfigurationRequest.getEnvironment(), null)


        val newConfigurations: MutableMap<ConfigResource, Config> = mutableMapOf()


        for (topicConfiguration : UpdateTopicConfiguration in updateTopicConfigurationRequest.getNewConfigurations()) {
            val updateRequestPair = updateTopicConfigurationConverter.convertTopicConfiguration(topicConfiguration)
            newConfigurations[updateRequestPair.first] = updateRequestPair.second
        }


        adminClient?.alterConfigs(newConfigurations) ?: log.error("Failed to create new configurations on request {}:{} because admin-client value was invalid or null.",
                updateTopicConfigurationRequest.getEnvironment(),
                updateTopicConfigurationRequest.getNewConfigurations().size)
    }

    fun updateTopicPartitionsFromRequest(updateTopicPartitionsRequest: UpdateTopicPartitionsRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(updateTopicPartitionsRequest.getEnvironment(), null)


        val newPartitions: Map<String, NewPartitions> = updateTopicPartitionsRequest.getNewPartitions().map {
            updateTopicPartitionsConverter.convertTopicPartitions(it)
        }.toMap()


        adminClient?.createPartitions(newPartitions) ?: log.error("Failed to create new partitions on request {}:{} because admin-client value was invalid or null.",
                updateTopicPartitionsRequest.getEnvironment(),
                updateTopicPartitionsRequest.getNewPartitions().size)
    }

    fun deleteTopicsFromRequest(deleteTopicRequest: DeleteTopicRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(deleteTopicRequest.getEnvironment(), null)

/*
        no converter needed!
 */
        val deleteTopics = deleteTopicRequest.getDeleteTopic().map { it.getTopicName() }


        adminClient?.deleteTopics(deleteTopics) ?: log.error("Failed to delete topics on request {}:{} because admin-client value was invalid or null.",
                deleteTopicRequest.getEnvironment(),
                deleteTopicRequest.getDeleteTopic().size)

    }

    fun deleteAclsFromRequest(deleteResourceAclRequest: DeleteResourceAclRequest) {

        val adminClient = adminClusters.adminClients.getOrDefault(deleteResourceAclRequest.getEnvironment(), null)

        val deleteAclFilters = deleteResourceAclRequest.getDeleteAcls().map {
            deleteResourceAclConverter.convertDeleteResourceAcl(it)
        }

        adminClient?.deleteAcls(deleteAclFilters) ?: log.error("Failed to delete acls on request {}:{} because admin-client value was invalid or null.",
                deleteResourceAclRequest.getEnvironment(),
                deleteResourceAclRequest.getDeleteAcls().size)
    }
}