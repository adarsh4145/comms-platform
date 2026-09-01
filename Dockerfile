# syntax=docker/dockerfile:1

# Runtime-only image, shared by every service.
#
#   mvn -q package -DskipTests
#   docker build --build-arg MODULE=ingestionService -t comms-platform/ingestion-service .
#
# Deliberately runtime-only: a builder stage would re-resolve the whole dependency tree on
# every image build, and the Maven reactor has already produced the jar. Configuration comes
# from the config server, so the image itself is environment-agnostic - point it at its
# dependencies with CONFIG_SERVER_URL / EUREKA_SERVER_URL at run time.

FROM eclipse-temurin:25-jre

ARG MODULE
ARG VERSION=0.0.1-SNAPSHOT

RUN useradd --create-home --shell /usr/sbin/nologin comms
WORKDIR /app

COPY --chown=comms:comms ${MODULE}/target/${MODULE}-${VERSION}.jar /app/application.jar

USER comms

# Containers get a memory limit, not a machine; let the JVM size its heap from the cgroup.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/application.jar"]
