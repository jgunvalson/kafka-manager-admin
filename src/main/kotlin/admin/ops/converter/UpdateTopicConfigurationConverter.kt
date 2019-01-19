package admin.ops.converter

import kafkamanager.model.UpdateTopicConfiguration
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


@Component
class UpdateTopicConfigurationConverter {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UpdateTopicConfigurationConverter::class.java)

    }


    fun convertTopicConfiguration(updateTopicConfiguration: UpdateTopicConfiguration): Pair<ConfigResource, Config> {

        val configResource = ConfigResource(ConfigResource.Type.TOPIC, updateTopicConfiguration.getTopicName())


        val configs = Config(updateTopicConfiguration.getConfiguration().map {
            ConfigEntry(it.getConfig(), it.getValue())
        })


        return Pair(configResource, configs)
    }
}