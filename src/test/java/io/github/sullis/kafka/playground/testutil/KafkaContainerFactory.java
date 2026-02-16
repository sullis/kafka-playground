package io.github.sullis.kafka.playground.testutil;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Factory class for creating Kafka container instances for testing.
 * Provides convenient methods to create different types of Kafka containers
 * (Apache Kafka, Apache Kafka Native, Confluent Platform).
 */
public class KafkaContainerFactory {
    
    /**
     * Default Apache Kafka version.
     */
    public static final String DEFAULT_APACHE_KAFKA_VERSION = "4.1.0";
    
    /**
     * Default Confluent Platform version.
     */
    public static final String DEFAULT_CONFLUENT_VERSION = "7.8.0";
    
    /**
     * Kafka container types supported by the factory.
     */
    public enum ContainerType {
        APACHE_KAFKA,
        APACHE_KAFKA_NATIVE,
        CONFLUENT_PLATFORM
    }
    
    /**
     * Creates a KafkaContainer with Apache Kafka using the default version.
     * 
     * @return a new KafkaContainer instance
     */
    public static KafkaContainer createApacheKafka() {
        return createApacheKafka(DEFAULT_APACHE_KAFKA_VERSION);
    }
    
    /**
     * Creates a KafkaContainer with Apache Kafka using a specific version.
     * 
     * @param version the Apache Kafka version to use
     * @return a new KafkaContainer instance
     */
    public static KafkaContainer createApacheKafka(String version) {
        return new KafkaContainer(DockerImageName.parse("apache/kafka:" + version));
    }
    
    /**
     * Creates a KafkaContainer with Apache Kafka Native image using the default version.
     * 
     * @return a new KafkaContainer instance
     */
    public static KafkaContainer createApacheKafkaNative() {
        return createApacheKafkaNative(DEFAULT_APACHE_KAFKA_VERSION);
    }
    
    /**
     * Creates a KafkaContainer with Apache Kafka Native image using a specific version.
     * 
     * @param version the Apache Kafka Native version to use
     * @return a new KafkaContainer instance
     */
    public static KafkaContainer createApacheKafkaNative(String version) {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:" + version));
    }
    
    /**
     * Creates a ConfluentKafkaContainer with Confluent Platform using the default version.
     * 
     * @return a new ConfluentKafkaContainer instance
     */
    public static ConfluentKafkaContainer createConfluentPlatform() {
        return createConfluentPlatform(DEFAULT_CONFLUENT_VERSION);
    }
    
    /**
     * Creates a ConfluentKafkaContainer with Confluent Platform using a specific version.
     * 
     * @param version the Confluent Platform version to use
     * @return a new ConfluentKafkaContainer instance
     */
    public static ConfluentKafkaContainer createConfluentPlatform(String version) {
        return new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + version));
    }
    
    /**
     * Creates a Kafka container of the specified type using default versions.
     * 
     * @param type the type of container to create
     * @return a new GenericContainer instance (KafkaContainer or ConfluentKafkaContainer)
     */
    public static GenericContainer<?> createContainer(ContainerType type) {
        return switch (type) {
            case APACHE_KAFKA -> createApacheKafka();
            case APACHE_KAFKA_NATIVE -> createApacheKafkaNative();
            case CONFLUENT_PLATFORM -> createConfluentPlatform();
        };
    }
    
    /**
     * Creates a Kafka container of the specified type using a specific version.
     * 
     * @param type the type of container to create
     * @param version the version to use
     * @return a new GenericContainer instance (KafkaContainer or ConfluentKafkaContainer)
     */
    public static GenericContainer<?> createContainer(ContainerType type, String version) {
        return switch (type) {
            case APACHE_KAFKA -> createApacheKafka(version);
            case APACHE_KAFKA_NATIVE -> createApacheKafkaNative(version);
            case CONFLUENT_PLATFORM -> createConfluentPlatform(version);
        };
    }
    
    /**
     * Builder for creating Kafka containers with custom configurations.
     */
    public static class Builder {
        private ContainerType type = ContainerType.APACHE_KAFKA;
        private String version;
        
        /**
         * Sets the container type.
         * 
         * @param type the container type
         * @return this builder
         */
        public Builder type(ContainerType type) {
            this.type = type;
            return this;
        }
        
        /**
         * Sets the version.
         * 
         * @param version the version to use
         * @return this builder
         */
        public Builder version(String version) {
            this.version = version;
            return this;
        }
        
        /**
         * Builds and returns the configured Kafka container.
         * 
         * @return a new GenericContainer instance
         */
        public GenericContainer<?> build() {
            if (version != null) {
                return createContainer(type, version);
            } else {
                return createContainer(type);
            }
        }
    }
    
    /**
     * Creates a new builder for constructing Kafka containers.
     * 
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
}
