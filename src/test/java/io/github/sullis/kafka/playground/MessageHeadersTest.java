package io.github.sullis.kafka.playground;

import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Kafka message headers functionality.
 */
public class MessageHeadersTest {
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
  public void testProducerAndConsumerWithHeaders() throws Exception {
    String topic = "test-headers-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);

    // Produce message with headers
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      var record = new ProducerRecord<>(topic, "key1", "value1");
      record.headers().add("correlation-id", "12345".getBytes(StandardCharsets.UTF_8));
      record.headers().add("request-source", "test-client".getBytes(StandardCharsets.UTF_8));
      record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
      
      var metadata = producer.send(record).get();
      assertThat(metadata.topic()).isEqualTo(topic);
    }

    // Consume message and verify headers
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
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
          assertThat(allRecords).hasSize(1);
        });

    consumer.close();

    // Verify headers
    ConsumerRecord<String, String> receivedRecord = allRecords.get(0);
    Headers headers = receivedRecord.headers();
    
    assertThat(headers).isNotNull();
    
    Header correlationId = headers.lastHeader("correlation-id");
    assertThat(correlationId).isNotNull();
    assertThat(new String(correlationId.value(), StandardCharsets.UTF_8)).isEqualTo("12345");
    
    Header requestSource = headers.lastHeader("request-source");
    assertThat(requestSource).isNotNull();
    assertThat(new String(requestSource.value(), StandardCharsets.UTF_8)).isEqualTo("test-client");
    
    Header timestamp = headers.lastHeader("timestamp");
    assertThat(timestamp).isNotNull();
    assertThat(new String(timestamp.value(), StandardCharsets.UTF_8)).matches("\\d+");
  }

  @Test
  public void testMultipleHeadersWithSameKey() throws Exception {
    String topic = "test-multiple-headers-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);

    // Produce message with multiple headers with same key
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      var record = new ProducerRecord<>(topic, "key1", "value1");
      record.headers().add("tag", "production".getBytes(StandardCharsets.UTF_8));
      record.headers().add("tag", "critical".getBytes(StandardCharsets.UTF_8));
      record.headers().add("tag", "urgent".getBytes(StandardCharsets.UTF_8));
      
      producer.send(record).get();
    }

    // Consume and verify
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
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
          assertThat(allRecords).hasSize(1);
        });

    consumer.close();

    // Verify multiple headers with same key
    ConsumerRecord<String, String> receivedRecord = allRecords.get(0);
    Headers headers = receivedRecord.headers();
    
    Iterable<Header> tagHeaders = headers.headers("tag");
    List<String> tagValues = new ArrayList<>();
    tagHeaders.forEach(h -> tagValues.add(new String(h.value(), StandardCharsets.UTF_8)));
    
    assertThat(tagValues).containsExactly("production", "critical", "urgent");
  }

  @Test
  public void testEmptyHeaderValue() throws Exception {
    String topic = "test-empty-header-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);

    // Produce message with empty header value
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      var record = new ProducerRecord<>(topic, "key1", "value1");
      record.headers().add("empty-header", new byte[0]);
      
      producer.send(record).get();
    }

    // Consume and verify
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringDeserializer.class);
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
          assertThat(allRecords).hasSize(1);
        });

    consumer.close();

    // Verify empty header
    ConsumerRecord<String, String> receivedRecord = allRecords.get(0);
    Header emptyHeader = receivedRecord.headers().lastHeader("empty-header");
    
    assertThat(emptyHeader).isNotNull();
    assertThat(emptyHeader.value()).isEmpty();
  }
}
