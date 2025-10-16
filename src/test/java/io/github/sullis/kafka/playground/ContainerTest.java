package io.github.sullis.kafka.playground;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
@ParameterizedClass(name = "{0}")
@MethodSource("argProvider")
record ContainerTest(String name, GenericContainer container) {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerTest.class);
  private static final String APACHE_KAFKA_VERSION = "4.1.0";
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

    final String bootstrapServers = "127.0.0.1:" + container.getFirstMappedPort();
    final Class serializerClass = org.apache.kafka.common.serialization.StringSerializer.class;
    final Class deserializerClass = org.apache.kafka.common.serialization.StringDeserializer.class;
    final String clientId = UUID.randomUUID().toString();

    Properties producerConfig = new Properties();
    producerConfig.put("client.id", clientId);
    producerConfig.put("bootstrap.servers", bootstrapServers);
    producerConfig.put("acks", "all");

    producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, serializerClass);
    producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, serializerClass);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      var record = new ProducerRecord<>("topic123", "Hello", "World");
      var metadata = producer.send(record).get();
      assertThat(metadata.topic()).isEqualTo("topic123");
    }

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroupId");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer(consumerConfig);
    consumer.subscribe(List.of("topic123"));

    List<ConsumerRecord> allRecords = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          LOGGER.info("pollResult count=" + pollResult.count());
          if (pollResult.count() > 0) {
            pollResult.iterator()
                .forEachRemaining(r -> allRecords.add((ConsumerRecord) r));
          }
          assertThat(allRecords).hasSize(1);
        });

    consumer.close();

    container.stop();
    container.close();
  }
}
