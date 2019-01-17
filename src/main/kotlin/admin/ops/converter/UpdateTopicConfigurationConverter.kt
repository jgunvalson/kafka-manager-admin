package admin.ops.converter

import kafkamanager.model.UpdateTopicConfigurationRequest
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class UpdateTopicConfigurationConverter {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UpdateTopicConfigurationConverter::class.java)

    }


    fun convertTopicConfiguration(updateTopicConfigurationRequest: UpdateTopicConfigurationRequest): Pair<ConfigResource, Config> {

        val configResource = ConfigResource(ConfigResource.Type.TOPIC, updateTopicConfigurationRequest.getTopicName())


        val configs = Config(updateTopicConfigurationRequest.getConfiguration().map {
            ConfigEntry(it.getConfig(), it.getValue())
        })


        return Pair(configResource, configs)
    }
}