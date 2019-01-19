package admin.config.clusters

import admin.config.AdminClientClusterConfig
import org.apache.kafka.clients.admin.AdminClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*


/*
    The purpose of this class is to generate the appropriate client beans for managing clusters. Still WIP.
 */
@Component
class AdminClusters constructor(@Autowired val adminClientClusterConfig: AdminClientClusterConfig) {


    val adminClients: Map<String, AdminClient> = adminClientClusterConfig.adminClientClusters.map {
        val props: Properties = Properties()
        props.putAll(it.configOverrides)
        it.environmentName to AdminClient.create(props)
    }.toMap()


}