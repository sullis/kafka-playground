package io.github.sullis.kafka.playground;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
 * Tests for different serialization formats.
 */
public class SerializationTest {
  private static final String KAFKA_VERSION = "4.1.0";
  private KafkaContainer kafka;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp() {
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:" + KAFKA_VERSION));
    kafka.start();
    objectMapper = new ObjectMapper();
  }

  @AfterEach
  public void tearDown() {
    if (kafka != null) {
      kafka.stop();
      kafka.close();
    }
  }

  /**
   * Simple JSON serializer for testing.
   */
  public static class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
      if (data == null) {
        return null;
      }
      try {
        return mapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException("Error serializing JSON", e);
      }
    }
  }

  /**
   * Simple JSON deserializer for testing.
   */
  public static class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> type;

    public JsonDeserializer(Class<T> type) {
      this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }
      try {
        return mapper.readValue(data, type);
      } catch (Exception e) {
        throw new RuntimeException("Error deserializing JSON", e);
      }
    }
  }

  /**
   * Test data class.
   */
  public static class TestData {
    private String id;
    private String name;
    private int value;

    public TestData() {}

    public TestData(String id, String name, int value) {
      this.id = id;
      this.name = name;
      this.value = value;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public int getValue() { return value; }
    public void setValue(int value) { this.value = value; }
  }

  @Test
  public void testJsonSerialization() throws Exception {
    String topic = "test-json-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    // Produce JSON messages
    try (var producer = new KafkaProducer<String, TestData>(producerConfig)) {
      var testData = new TestData("123", "test-name", 42);
      var record = new ProducerRecord<>(topic, "key1", testData);
      producer.send(record).get();
    }

    // Consume JSON messages
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer<>(consumerConfig, 
        new StringDeserializer(), 
        new JsonDeserializer<>(TestData.class));
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<String, TestData>> allRecords = new ArrayList<>();

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

    // Verify deserialized data
    TestData receivedData = allRecords.get(0).value();
    assertThat(receivedData.getId()).isEqualTo("123");
    assertThat(receivedData.getName()).isEqualTo("test-name");
    assertThat(receivedData.getValue()).isEqualTo(42);
  }

  @Test
  public void testByteArraySerialization() throws Exception {
    String topic = "test-bytes-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.ByteArraySerializer.class);

    // Produce byte array messages
    try (var producer = new KafkaProducer<byte[], byte[]>(producerConfig)) {
      byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
      byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
      var record = new ProducerRecord<>(topic, key, value);
      producer.send(record).get();
    }

    // Consume byte array messages
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    var consumer = new KafkaConsumer<byte[], byte[]>(consumerConfig);
    consumer.subscribe(List.of(topic));

    List<ConsumerRecord<byte[], byte[]>> allRecords = new ArrayList<>();

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

    // Verify byte arrays
    ConsumerRecord<byte[], byte[]> record = allRecords.get(0);
    assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("test-key");
    assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("test-value");
  }

  @Test
  public void testNullKeyAndValue() throws Exception {
    String topic = "test-null-topic";
    
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    // Produce message with null key
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
      var record = new ProducerRecord<String, String>(topic, null, "value-only");
      producer.send(record).get();
    }

    // Consume messages
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
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
          assertThat(allRecords).hasSize(1);
        });

    consumer.close();

    // Verify null key
    ConsumerRecord<String, String> record = allRecords.get(0);
    assertThat(record.key()).isNull();
    assertThat(record.value()).isEqualTo("value-only");
  }
}
