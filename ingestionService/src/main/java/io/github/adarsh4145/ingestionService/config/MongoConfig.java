package io.github.adarsh4145.ingestionService.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.micrometer.observation.ObservationRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.observability.ContextProviderFactory;
import org.springframework.data.mongodb.observability.MongoObservationCommandListener;
import org.springframework.transaction.reactive.TransactionalOperator;

/***
 * Creating this due to known issue of spring mongo db, it gives auth error
 * when using autoconfiguration.
 *
 * <p>Because the client is built here rather than by Boot, none of Boot's
 * MongoClientSettingsBuilderCustomizers apply — including the one that installs observation. The
 * command listener and context provider below put that back, so Mongo commands appear as spans in
 * the same trace as the request that issued them.
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
  public MongoClient reactiveMongoClient(ObservationRegistry observationRegistry) {
    String uri = environment.getRequiredProperty(SPRING_DATA_MONGODB_URI);
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(uri))
            .addCommandListener(new MongoObservationCommandListener(observationRegistry))
            .contextProvider(ContextProviderFactory.create(observationRegistry))
            .build();
    return MongoClients.create(settings);
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
