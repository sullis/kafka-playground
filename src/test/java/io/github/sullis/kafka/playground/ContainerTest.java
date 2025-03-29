package io.github.sullis.kafka.playground;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


/*

  Testcontainers Kafka module:

  https://java.testcontainers.org/modules/kafka/

 */
@ParameterizedClass
@MethodSource("argProvider")
record ContainerTest(String name, GenericContainer container) {
  private static final String APACHE_KAFKA_VERSION = "4.0.0";
  private static final String CONFLUENT_VERSION = "7.9.0";
  private static final KafkaContainer APACHE_KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:" + APACHE_KAFKA_VERSION));
  private static final KafkaContainer APACHE_KAFKA_NATIVE_IMAGE = new KafkaContainer(DockerImageName.parse("apache/kafka-native:" + APACHE_KAFKA_VERSION));
  private static final org.testcontainers.containers.KafkaContainer CONFLUENT_PLATFORM_KAFKA = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION));
  private static final org.testcontainers.containers.KafkaContainer CONFLUENT_PLATFORM_KAFKA_WITH_KRAFT = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION)).withKraft();

  private static Stream<Arguments> argProvider() {
    return Stream.of(
        arguments("ApacheKafka", APACHE_KAFKA),
        arguments("ApacheKafkaNativeImage", APACHE_KAFKA_NATIVE_IMAGE),
        arguments("ConfluentKafka", CONFLUENT_PLATFORM_KAFKA));
  }

  @Test
  public void validate() {
    container.start();
    assertThat(container.isRunning()).isTrue();
    container.stop();
    container.close();
  }
}
