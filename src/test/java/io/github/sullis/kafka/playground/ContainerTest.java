package io.github.sullis.kafka.playground;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


/*

  Testcontainers Kafka module:

  https://java.testcontainers.org/modules/kafka/

 */
public class ContainerTest {
  private static final String APACHE_KAFKA_VERSION = "4.0.0";
  private static final String CONFLUENT_VERSION = "7.9.0";
  private static final KafkaContainer APACHE_KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:" + APACHE_KAFKA_VERSION));
  private static final KafkaContainer APACHE_KAFKA_NATIVE_IMAGE = new KafkaContainer(DockerImageName.parse("apache/kafka-native:" + APACHE_KAFKA_VERSION));
  private static final org.testcontainers.containers.KafkaContainer CONFLUENT_PLATFORM_KAFKA = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION));
  private static final org.testcontainers.containers.KafkaContainer CONFLUENT_PLATFORM_KAFKA_WITH_KRAFT = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION)).withKraft();

  @Test
  public void testConfluentPlatformKafka() {
    validate(CONFLUENT_PLATFORM_KAFKA);
  }

  @Test
  public void testConfluentPlatformKafkaWithKraft() {
    validate(CONFLUENT_PLATFORM_KAFKA_WITH_KRAFT);
  }

  @Test
  public void testApacheKafka() {
    validate(APACHE_KAFKA);
  }

  @Test
  public void testApacheKafkaNativeImage() {
    validate(APACHE_KAFKA_NATIVE_IMAGE);
  }

  private void validate(GenericContainer<?> container) {
    container.start();
    container.stop();
    container.close();
  }
}
