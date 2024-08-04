package io.github.sullis.kafka.playground;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


/*

  Testcontainers Kafka module:

  https://java.testcontainers.org/modules/kafka/

 */
public class ContainerTest {
  private static final String APACHE_KAFKA_VERSION = "3.8.0";
  private static final KafkaContainer APACHE_KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:" + APACHE_KAFKA_VERSION));
  private static final KafkaContainer APACHE_KAFKA_NATIVE_IMAGE = new KafkaContainer(DockerImageName.parse("apache/kafka-native:" + APACHE_KAFKA_VERSION));
  private static final KafkaContainer CONFLUENT_PLATFORM_KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0").asCompatibleSubstituteFor("apache/kafka"));

  @Test
  @Disabled
  public void testConfluentPlatformKafka() {
    validate(CONFLUENT_PLATFORM_KAFKA);
  }

  @Test
  public void testApacheKafka() {
    validate(APACHE_KAFKA);
  }

  @Test
  public void testApacheKafkaNativeImage() {
    validate(APACHE_KAFKA_NATIVE_IMAGE);
  }

  private void validate(org.testcontainers.kafka.KafkaContainer container) {
    container.start();
    container.stop();
    container.close();
  }
}
