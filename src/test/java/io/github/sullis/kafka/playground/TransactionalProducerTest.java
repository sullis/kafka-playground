package io.github.sullis.kafka.playground;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Kafka transactional producer functionality.
 */
public class TransactionalProducerTest {
  private static final String KAFKA_VERSION = "4.1.0";
  private KafkaContainer kafka;

  @BeforeEach
  public void setUp() {
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:" + KAFKA_VERSION));
    kafka.start();
  }

  @AfterEach
  public void tearDown() {
    if (kafka != null) {
      kafka.stop();
      kafka.close();
    }
  }

  @Test
  public void testTransactionalProducerCommit() throws Exception {
    String topic = "test-transactional-topic";
    String transactionalId = "test-tx-" + UUID.randomUUID();
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    // Produce messages in a transaction
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      producer.initTransactions();
      
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(topic, "key1", "value1"));
      producer.send(new ProducerRecord<>(topic, "key2", "value2"));
      producer.send(new ProducerRecord<>(topic, "key3", "value3"));
      producer.commitTransaction();
    }

    // Consume messages with read_committed isolation
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(allRecords::add);
          }
          assertThat(allRecords).hasSize(3);
        });

    consumer.close();

    // Verify all messages were received
    assertThat(allRecords).hasSize(3);
    assertThat(allRecords.get(0).value()).isEqualTo("value1");
    assertThat(allRecords.get(1).value()).isEqualTo("value2");
    assertThat(allRecords.get(2).value()).isEqualTo("value3");
  }

  @Test
  public void testTransactionalProducerAbort() throws Exception {
    String topic = "test-transactional-abort-topic";
    String transactionalId = "test-tx-abort-" + UUID.randomUUID();
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    // Produce messages in a transaction and abort
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      producer.initTransactions();
      
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(topic, "key1", "value1"));
      producer.send(new ProducerRecord<>(topic, "key2", "value2"));
      producer.abortTransaction();
      
      // Send messages in a new committed transaction
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(topic, "key3", "value3"));
      producer.commitTransaction();
    }

    // Consume messages with read_committed isolation
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(allRecords::add);
          }
          assertThat(allRecords).hasSizeGreaterThanOrEqualTo(1);
        });

    consumer.close();

    // Verify only the committed message was received (aborted messages should not be visible)
    assertThat(allRecords).hasSize(1);
    assertThat(allRecords.get(0).value()).isEqualTo("value3");
  }

  @Test
  public void testReadUncommittedSeeAllMessages() throws Exception {
    String topic = "test-read-uncommitted-topic";
    String transactionalId = "test-tx-uncommitted-" + UUID.randomUUID();
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    // Produce messages in a transaction and abort
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      producer.initTransactions();
      
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(topic, "key1", "value1"));
      producer.send(new ProducerRecord<>(topic, "key2", "value2"));
      producer.abortTransaction();
      
      // Send messages in a new committed transaction
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(topic, "key3", "value3"));
      producer.commitTransaction();
    }

    // Consume messages with read_uncommitted isolation (default)
    // Note: Even with read_uncommitted, aborted messages are filtered out by the consumer
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(allRecords::add);
          }
          assertThat(allRecords).hasSizeGreaterThanOrEqualTo(1);
        });

    consumer.close();

    // Verify at least the committed message was received
    assertThat(allRecords).hasSizeGreaterThanOrEqualTo(1);
    assertThat(allRecords.stream().map(ConsumerRecord::value))
        .contains("value3");
  }
}
