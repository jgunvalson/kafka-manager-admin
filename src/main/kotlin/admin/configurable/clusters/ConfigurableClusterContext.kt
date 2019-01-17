package admin.configurable.clusters

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import javax.annotation.PostConstruct


/*
    The purpose of this class is to generate the appropriate client beans for managing clusters. Still WIP.
 */
@ConfigurationProperties(prefix = "clusters")
class ConfigurableClusterContext{

    companion object {
        private val log: Logger = LoggerFactory.getLogger(ConfigurableClusterContext::class.java)
    }

    @Autowired
    val applicationContext: ApplicationContext? = null

    @PostConstruct
    fun init(): Unit {
        log.info("Attempting to initialize Client beans for defined cluster objects.")
        val configurableApplicationContext = applicationContext as ConfigurableApplicationContext
        val beanFactory: ConfigurableListableBeanFactory = configurableApplicationContext.beanFactory
        val clusters = mutableMapOf<String, Any>()
        clusters.map {
            log.info("Found cluster with name {}.", it.key)
            log.info("The value {}.", it.value)
//            beanFactory.registerSingleton()
        }
    }
}