package io.github.adarsh4145.ingestionService.config;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;

/***
 * Creating this due to known issue of spring mongo db, it gives auth error
 * when using autoconfiguration
 */

@Configuration
@Slf4j
public class MongoConfig {

  public static final String SPRING_DATA_MONGODB_URI = "spring.data.mongodb.uri";

  private final Environment environment;

  public MongoConfig(Environment environment) {
    this.environment = environment;
  }

  @Bean
  public MongoClient reactiveMongoClient() {
    String uri = environment.getRequiredProperty(SPRING_DATA_MONGODB_URI);
    return MongoClients.create(new ConnectionString(uri));
  }

  @Bean
  public ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory(
      MongoClient reactiveMongoClient) {
    String uri = environment.getRequiredProperty(SPRING_DATA_MONGODB_URI);
    ConnectionString connectionString = new ConnectionString(uri);
    return new SimpleReactiveMongoDatabaseFactory(
        reactiveMongoClient, connectionString.getDatabase());
  }

  @Bean
  public ReactiveMongoTemplate reactiveMongoTemplate(
      ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory) {
    return new ReactiveMongoTemplate(reactiveMongoDatabaseFactory);
  }

  @Bean
  public ReactiveMongoTransactionManager transactionManager(
      ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory) {
    return new ReactiveMongoTransactionManager(reactiveMongoDatabaseFactory);
  }

  @Bean
  public TransactionalOperator transactionalOperator(
      ReactiveMongoTransactionManager transactionManager) {
    return TransactionalOperator.create(transactionManager);
  }
}
