package io.github.sullis.kafka.playground;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import io.github.sullis.kafka.playground.testutil.KafkaContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Kafka producer error handling scenarios.
 */
public class ProducerErrorHandlingTest {
  private KafkaContainer kafka;

  @BeforeEach
  public void setUp() {
    kafka = KafkaContainerFactory.createApacheKafka();
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
  public void testInvalidBootstrapServers() {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092"); // Valid format but wrong port
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
    config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);

    try (var producer = new KafkaProducer<String, String>(config)) {
      var record = new ProducerRecord<>("test-topic", "key", "value");
      assertThatThrownBy(() -> producer.send(record).get())
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(TimeoutException.class);
    }
  }

  @Test
  public void testProducerWithInvalidSerializer() {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        "org.apache.kafka.common.serialization.InvalidSerializer");

    assertThatThrownBy(() -> new KafkaProducer<String, String>(config))
        .isInstanceOf(KafkaException.class);
  }

  @Test
  public void testProducerFlush() throws Exception {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 10000); // Long linger time

    try (var producer = new KafkaProducer<String, String>(config)) {
      var record = new ProducerRecord<>("test-topic", "key", "value");
      producer.send(record);
      
      // Flush should ensure message is sent
      long startTime = System.currentTimeMillis();
      producer.flush();
      long duration = System.currentTimeMillis() - startTime;
      
      // Flush should complete quickly (not wait for linger time)
      assertThat(duration).isLessThan(5000);
    }
  }

  @Test
  public void testProducerClose() throws Exception {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);

    var producer = new KafkaProducer<String, String>(config);
    var record = new ProducerRecord<>("test-topic", "key", "value");
    producer.send(record);
    
    // Close with timeout
    producer.close(Duration.ofSeconds(5));
    
    // Further operations should fail
    assertThatThrownBy(() -> producer.send(record))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testProducerMetrics() {
    Properties config = new Properties();
    config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
        org.apache.kafka.common.serialization.StringSerializer.class);

    try (var producer = new KafkaProducer<String, String>(config)) {
      var metrics = producer.metrics();
      
      // Verify metrics are available
      assertThat(metrics).isNotEmpty();
      assertThat(metrics.keySet()).isNotEmpty();
    }
  }
}
