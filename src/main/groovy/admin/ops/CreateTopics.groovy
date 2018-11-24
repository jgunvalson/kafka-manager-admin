package admin.ops

import kafkamanager.model.TopicRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component

@Component
class CreateTopics {

    @Autowired
    KafkaProperties kafkaProperties

    void CreateTopicsFromRequests(ArrayList<TopicRequest> topicRequests){
        Properties adminConfiguration = new Properties()
        adminConfiguration.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.consumer.bootstrapServers)
        adminConfiguration.put(AdminClientConfig.RETRIES_CONFIG, "10")
        AdminClient client = AdminClient.create(adminConfiguration)

        ArrayList<NewTopic> newTopics = topicRequests.stream()
            .map{ t -> new NewTopic(t.topicName, t.partitions, Short(t.replication)) }
            .collect()
            .toList()

        client.createTopics(newTopics)
    }
}
