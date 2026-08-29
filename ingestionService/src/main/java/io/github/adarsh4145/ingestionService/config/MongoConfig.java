package io.github.adarsh4145.ingestionService.config;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;

/***
 * Creating this due to known issue of spring mongo db, it gives auth error
 * when using autoconfiguration
 */

@Configuration
@Slf4j
public class MongoConfig {

    private final Environment environment;

    public MongoConfig(Environment environment) {
        this.environment = environment;
        log.info("Mongo URL Loaded: {}",environment.getRequiredProperty("spring.data.mongodb.uri"));
    }

    @Bean
    public MongoClient reactiveMongoClient() {
        String uri = environment.getRequiredProperty("spring.data.mongodb.uri");
        return MongoClients.create(new ConnectionString(uri));
    }

    @Bean
    public ReactiveMongoTemplate reactiveMongoTemplate(MongoClient reactiveMongoClient) {
        String uri = environment.getRequiredProperty("spring.data.mongodb.uri");
        ConnectionString connectionString = new ConnectionString(uri);
        return new ReactiveMongoTemplate(
                new SimpleReactiveMongoDatabaseFactory(reactiveMongoClient, connectionString.getDatabase())
        );
    }
}