package admin

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
class KafkaManagerAdminApplication {

    static void main(String[] args) {
        SpringApplication.run(KafkaManagerAdminApplication.class, args);
    }
}
