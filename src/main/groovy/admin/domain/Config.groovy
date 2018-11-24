package admin.domain


import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "kafkamanager")
class Config {
    private String apiInputTopics;
}
