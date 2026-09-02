package io.github.adarsh4145.core;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * core is a library, not an application - it has no @SpringBootApplication for @SpringBootTest to
 * discover, so the configuration to load is named explicitly. This still exercises what matters
 * here: that the beans core contributes to every service actually wire up.
 */
@SpringBootTest(classes = CoreAutoConfiguration.class)
class CoreApplicationTests {

  @Test
  void contextLoads() {}
}
