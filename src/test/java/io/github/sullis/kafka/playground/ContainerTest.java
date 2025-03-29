package io.github.sullis.kafka.playground;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
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
  private static final String CONFLUENT_VERSION = "7.8.0";
  private static final KafkaContainer APACHE_KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:" + APACHE_KAFKA_VERSION));
  private static final KafkaContainer APACHE_KAFKA_NATIVE_IMAGE = new KafkaContainer(DockerImageName.parse("apache/kafka-native:" + APACHE_KAFKA_VERSION));
  private static final ConfluentKafkaContainer CONFLUENT_PLATFORM_KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION));

  private static Stream<Arguments> argProvider() {
    return Stream.of(
        args(APACHE_KAFKA),
        args(APACHE_KAFKA_NATIVE_IMAGE),
        args(CONFLUENT_PLATFORM_KAFKA)
    );
  }

  private static Arguments args(GenericContainer genericCon) {
    return arguments(genericCon.getDockerImageName(), genericCon);
  }

  @Test
  public void validate() throws Exception {
    container.start();
    assertThat(container.isRunning()).isTrue();
    assertThat(container.getFirstMappedPort()).isGreaterThan(0);

    String bootstrapServer = "127.0.0.1:" + container.getFirstMappedPort();

    Properties config = new Properties();
    config.put("client.id", UUID.randomUUID().toString());
    config.put("bootstrap.servers", bootstrapServer);
    config.put("acks", "all");

    var serializerClassName = org.apache.kafka.common.serialization.StringSerializer.class.getName();
    config.put(KEY_SERIALIZER_CLASS_CONFIG, serializerClassName);
    config.put(VALUE_SERIALIZER_CLASS_CONFIG, serializerClassName);

    try (var producer = new KafkaProducer<String, String>(config)) {
      var record = new ProducerRecord<>("topic123", null, "Hello", "World");
      var metadata = producer.send(record).get();
      assertThat(metadata.topic()).isEqualTo("topic123");
    }

    container.stop();
    container.close();
  }
}
