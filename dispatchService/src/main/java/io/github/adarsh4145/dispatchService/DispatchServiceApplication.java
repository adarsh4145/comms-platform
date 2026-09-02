package io.github.adarsh4145.dispatchService;

import java.util.TimeZone;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class DispatchServiceApplication {

  public static void main(String[] args) {
    // The pg JDBC driver sends the JVM's default zone id verbatim as the "TimeZone" startup
    // parameter, and a name the server does not know is a FATAL, not a warning. On Windows the JDK
    // still reports the legacy "Asia/Calcutta", which Debian moved out of tzdata into
    // tzdata-legacy - so the postgres:18 image does not have it and refuses every connection.
    // Pinning UTC also keeps this service's log timestamps aligned with Tempo and Loki.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    SpringApplication.run(DispatchServiceApplication.class, args);
  }
}
