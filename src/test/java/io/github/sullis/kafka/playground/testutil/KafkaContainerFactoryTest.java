package io.github.sullis.kafka.playground.testutil;

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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;

import static io.github.sullis.kafka.playground.testutil.KafkaContainerFactory.ContainerType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for KafkaContainerFactory.
 */
public class KafkaContainerFactoryTest {
    
    @Test
    public void testCreateApacheKafkaWithDefaultVersion() {
        KafkaContainer container = KafkaContainerFactory.createApacheKafka();
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("apache/kafka");
        assertThat(container.getDockerImageName()).contains(KafkaContainerFactory.DEFAULT_APACHE_KAFKA_VERSION);
    }
    
    @Test
    public void testCreateApacheKafkaWithCustomVersion() {
        String customVersion = "4.0.0";
        KafkaContainer container = KafkaContainerFactory.createApacheKafka(customVersion);
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("apache/kafka:" + customVersion);
    }
    
    @Test
    public void testCreateApacheKafkaNativeWithDefaultVersion() {
        KafkaContainer container = KafkaContainerFactory.createApacheKafkaNative();
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("apache/kafka-native");
        assertThat(container.getDockerImageName()).contains(KafkaContainerFactory.DEFAULT_APACHE_KAFKA_VERSION);
    }
    
    @Test
    public void testCreateApacheKafkaNativeWithCustomVersion() {
        String customVersion = "4.0.0";
        KafkaContainer container = KafkaContainerFactory.createApacheKafkaNative(customVersion);
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("apache/kafka-native:" + customVersion);
    }
    
    @Test
    public void testCreateConfluentPlatformWithDefaultVersion() {
        ConfluentKafkaContainer container = KafkaContainerFactory.createConfluentPlatform();
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("confluentinc/cp-kafka");
        assertThat(container.getDockerImageName()).contains(KafkaContainerFactory.DEFAULT_CONFLUENT_VERSION);
    }
    
    @Test
    public void testCreateConfluentPlatformWithCustomVersion() {
        String customVersion = "7.7.0";
        ConfluentKafkaContainer container = KafkaContainerFactory.createConfluentPlatform(customVersion);
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains("confluentinc/cp-kafka:" + customVersion);
    }
    
    @Test
    public void testCreateContainerByType() {
        GenericContainer<?> apacheKafka = KafkaContainerFactory.createContainer(ContainerType.APACHE_KAFKA);
        assertThat(apacheKafka).isInstanceOf(KafkaContainer.class);
        assertThat(apacheKafka.getDockerImageName()).contains("apache/kafka");
        
        GenericContainer<?> apacheKafkaNative = KafkaContainerFactory.createContainer(ContainerType.APACHE_KAFKA_NATIVE);
        assertThat(apacheKafkaNative).isInstanceOf(KafkaContainer.class);
        assertThat(apacheKafkaNative.getDockerImageName()).contains("apache/kafka-native");
        
        GenericContainer<?> confluentPlatform = KafkaContainerFactory.createContainer(ContainerType.CONFLUENT_PLATFORM);
        assertThat(confluentPlatform).isInstanceOf(ConfluentKafkaContainer.class);
        assertThat(confluentPlatform.getDockerImageName()).contains("confluentinc/cp-kafka");
    }
    
    @Test
    public void testCreateContainerByTypeWithVersion() {
        String version = "4.0.0";
        GenericContainer<?> apacheKafka = KafkaContainerFactory.createContainer(ContainerType.APACHE_KAFKA, version);
        assertThat(apacheKafka).isInstanceOf(KafkaContainer.class);
        assertThat(apacheKafka.getDockerImageName()).contains("apache/kafka:" + version);
    }
    
    @Test
    public void testBuilderWithDefaults() {
        GenericContainer<?> container = KafkaContainerFactory.builder().build();
        assertThat(container).isNotNull();
        assertThat(container).isInstanceOf(KafkaContainer.class);
        assertThat(container.getDockerImageName()).contains("apache/kafka");
    }
    
    @Test
    public void testBuilderWithType() {
        GenericContainer<?> container = KafkaContainerFactory.builder()
            .type(ContainerType.CONFLUENT_PLATFORM)
            .build();
        assertThat(container).isNotNull();
        assertThat(container).isInstanceOf(ConfluentKafkaContainer.class);
        assertThat(container.getDockerImageName()).contains("confluentinc/cp-kafka");
    }
    
    @Test
    public void testBuilderWithVersion() {
        String version = "4.0.0";
        GenericContainer<?> container = KafkaContainerFactory.builder()
            .version(version)
            .build();
        assertThat(container).isNotNull();
        assertThat(container.getDockerImageName()).contains(version);
    }
    
    @Test
    public void testBuilderWithTypeAndVersion() {
        String version = "7.7.0";
        GenericContainer<?> container = KafkaContainerFactory.builder()
            .type(ContainerType.CONFLUENT_PLATFORM)
            .version(version)
            .build();
        assertThat(container).isNotNull();
        assertThat(container).isInstanceOf(ConfluentKafkaContainer.class);
        assertThat(container.getDockerImageName()).contains("confluentinc/cp-kafka:" + version);
    }
    
    @Test
    public void testFactoryCreatedContainerCanProduceAndConsume() throws Exception {
        // Create container using factory
        KafkaContainer kafka = KafkaContainerFactory.createApacheKafka();
        kafka.start();
        
        try {
            String topic = "test-factory-topic";
            
            // Producer configuration
            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            
            // Produce a message
            try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                var record = new ProducerRecord<>(topic, "test-key", "test-value");
                producer.send(record).get();
            }
            
            // Consumer configuration
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
            
            // Consume the message
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
            
            // Verify the message
            assertThat(allRecords).hasSize(1);
            assertThat(allRecords.get(0).key()).isEqualTo("test-key");
            assertThat(allRecords.get(0).value()).isEqualTo("test-value");
            
        } finally {
            kafka.stop();
            kafka.close();
        }
    }
}
