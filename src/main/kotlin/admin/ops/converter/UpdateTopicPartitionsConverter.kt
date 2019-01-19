package admin.ops.converter

import kafkamanager.model.UpdateTopicPartitions
import org.apache.kafka.clients.admin.NewPartitions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class UpdateTopicPartitionsConverter {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UpdateTopicPartitionsConverter::class.java)
    }


    fun convertTopicPartitions(updateTopicPartitions: UpdateTopicPartitions): Pair<String, NewPartitions> {

        return Pair(updateTopicPartitions.getTopicName(), NewPartitions.increaseTo(updateTopicPartitions.getPartitions()))
    }
}