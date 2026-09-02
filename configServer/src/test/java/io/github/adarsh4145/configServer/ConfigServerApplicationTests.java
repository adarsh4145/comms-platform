package io.github.adarsh4145.configServer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Eureka is switched off here rather than in a test application.yaml. configServer needs the
 * "native" profile and its search-locations from the real application.yaml to start at all, and a
 * test resource of the same name would shadow that file rather than merge with it.
 *
 * <p>Without this the eureka client spent the test trying to reach localhost:8025 and logged a
 * connection-refused stack trace on every build - noisy, and misleading, since the test passes.
 */
@SpringBootTest(properties = "eureka.client.enabled=false")
class ConfigServerApplicationTests {

  @Test
  void contextLoads() {}
}
