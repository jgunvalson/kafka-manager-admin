package admin.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration


@Configuration
@ConfigurationProperties(prefix = "clusters")
open class AdminClientClusterConfig {


    lateinit var adminClientClusters: List<AdminClientCluster>


    companion object {
        class AdminClientCluster {

            lateinit var environmentName: String
            lateinit var configOverrides: Map<String, String>

        }
    }
}