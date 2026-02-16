package io.github.sullis.kafka.playground;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
 * Tests for Kafka consumer offset management and partition operations.
 */
public class ConsumerOffsetTest {
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
  public void testManualOffsetCommit() throws Exception {
    String topic = "test-manual-offset-topic";
    String groupId = "manual-commit-group";
    
    // Produce messages
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      for (int i = 0; i < 5; i++) {
        producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
      }
    }

    // Consume with manual commit
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

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
          assertThat(allRecords).hasSize(5);
        });

    // Manually commit offsets
    consumer.commitSync();
    
    // Verify committed offsets
    var assignment = consumer.assignment();
    assertThat(assignment).isNotEmpty();
    
    for (TopicPartition partition : assignment) {
      OffsetAndMetadata committed = consumer.committed(Collections.singleton(partition)).get(partition);
      assertThat(committed).isNotNull();
      assertThat(committed.offset()).isGreaterThan(0);
    }

    consumer.close();

    assertThat(allRecords).hasSize(5);
  }

  @Test
  public void testSeekToBeginning() throws Exception {
    String topic = "test-seek-beginning-topic";
    
    // Produce messages
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      for (int i = 0; i < 3; i++) {
        producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
      }
    }

    // First consume
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "seek-test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<String, String>> firstBatch = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(firstBatch::add);
          }
          assertThat(firstBatch).hasSize(3);
        });

    // Seek to beginning
    consumer.seekToBeginning(consumer.assignment());

    List<ConsumerRecord<String, String>> secondBatch = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(secondBatch::add);
          }
          assertThat(secondBatch).hasSize(3);
        });

    consumer.close();

    // Verify we read the same messages again
    assertThat(secondBatch).hasSize(3);
  }

  @Test
  public void testSeekToEnd() throws Exception {
    String topic = "test-seek-end-topic";
    
    // Consume and seek to end first, before producing messages
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "seek-end-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    consumer.subscribe(List.of(topic));

    // Poll once to get assignment
    consumer.poll(Duration.ofMillis(100));
    
    // Seek to end before messages are produced
    consumer.seekToEnd(consumer.assignment());
    
    // Now produce messages
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      for (int i = 0; i < 3; i++) {
        producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
      }
    }

    // Poll - should get new messages since we're at the end
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(records::add);
          }
          assertThat(records).hasSize(3);
        });

    consumer.close();
    
    // Verify we got the new messages
    assertThat(records).hasSize(3);
  }

  @Test
  public void testPartitionAssignment() throws Exception {
    String topic = "test-partition-assignment-topic";
    
    // Produce messages
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();
    }

    // Consume with manual partition assignment
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "assignment-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer<String, String>(consumerConfig);
    
    // Manually assign partition 0
    TopicPartition partition = new TopicPartition(topic, 0);
    consumer.assign(Collections.singletonList(partition));

    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
          var pollResult = consumer.poll(Duration.ofMillis(100));
          if (pollResult.count() > 0) {
            pollResult.forEach(allRecords::add);
          }
          assertThat(allRecords).hasSize(1);
        });

    // Verify assignment
    var assignment = consumer.assignment();
    assertThat(assignment).containsExactly(partition);

    consumer.close();
  }

  @Test
  public void testConsumerPosition() throws Exception {
    String topic = "test-position-topic";
    
    // Produce messages
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      for (int i = 0; i < 5; i++) {
        producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
      }
    }

    // Consume and check position
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "position-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
          assertThat(allRecords).hasSize(5);
        });

    // Check position for each partition
    for (TopicPartition partition : consumer.assignment()) {
      long position = consumer.position(partition);
      assertThat(position).isGreaterThan(0);
    }

    consumer.close();
  }
}
