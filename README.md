# kafka-playground

## Test Utilities

### KafkaContainerFactory

A factory class for creating Kafka container instances for testing. Provides convenient methods to create different types of Kafka containers with sensible defaults.

**Usage examples:**

```java
// Create Apache Kafka container with default version
KafkaContainer kafka = KafkaContainerFactory.createApacheKafka();

// Create Confluent Platform container with custom version
ConfluentKafkaContainer kafka = KafkaContainerFactory.createConfluentPlatform("7.7.0");

// Use builder pattern for flexibility
GenericContainer<?> kafka = KafkaContainerFactory.builder()
    .type(ContainerType.APACHE_KAFKA_NATIVE)
    .version("4.1.0")
    .build();
```

See [KafkaContainerFactoryTest.java](src/test/java/io/github/sullis/kafka/playground/testutil/KafkaContainerFactoryTest.java) for more examples.

---

- [Kafka time lag metrics](https://www.warpstream.com/blog/the-kafka-metric-youre-not-using-stop-counting-messages-start-measuring-time)
- [DoorDash: Kafka Self Service Platform](https://doordash.engineering/2024/08/13/doordash-engineers-with-kafka-self-serve/)
- [async processing with Kafka Streams](https://www.responsive.dev/blog/async-processing-for-kafka-streams)
- [kafka DLQ triage](https://skey.uk/post/kafka-dead-letter-queue-troubleshooting-guide/)

# Videos
- [Kafka meets Iceberg](https://www.youtube.com/watch?v=bzVjBu5nhyM) - Kasun Indrasiri  at GOTO Chicago 2024

# Diskless Kafka
- [Aiven: Diskless 2.0](https://aiven.io/blog/diskless-unified-zero-copy-apache-kafka) - September 2025
- [Aiven: Hitchhiker’s guide to Diskless Kafka](https://aiven.io/blog/guide-diskless-apache-kafka-kip-1150)
- [Aiven: Diskless Kafka is the tide](https://aiven.io/blog/diskless-kafka-is-the-tide-and-its-rising)
- [video: Ins and Outs of KIP-1150: Diskless Topics in Apache Kafka](https://www.youtube.com/watch?v=hrMvOFoQ3X4)
- [GeekNarrator: Diskless Kafka KIP-1150](https://www.geeknarrator.com/blog/diskless-kafka-kip-1150)

# Parallel Consumer
- [Confluent parallel consumer](https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/) - December 2020
- [github: parallel-consumer](https://github.com/confluentinc/parallel-consumer)
- [video: parallel-consumer](https://www.youtube.com/watch?v=mVYe_r0SBV8)

# WarpStream
- [Reimplementing Apache Kafka with Golang and S3](https://www.youtube.com/watch?v=xgzmxe6cj6A) - Ryan Worl, WarpStream
- [Beyond Kafka: Cutting Costs and Complexity with WarpStream and S3](https://www.youtube.com/watch?v=wgwUE2izH38) - Ryan Worl, WarpStream
- [Beyond Tiered Storage: Deep dive into WarpStream's Storage Engine](https://www.youtube.com/watch?v=74ZuGhNP3w8) - Richie Artoul, WarpStream @ Current 2024

# Low latency kafka
- [tier 1 bank tuning](https://www.confluent.io/blog/tier-1-bank-ultra-low-latency-trading-design/)

# Rebuild kafka from scratch?
- [Gunnar Morling blog post](https://www.morling.dev/blog/what-if-we-could-rebuild-kafka-from-scratch/)

# StreamNative Kafka
- [Ursa](https://streamnative.io/blog/ursa-reimagine-apache-kafka-for-the-cost-conscious-data-streaming)

# Kafka at Wix
- [The Wix Engineering 2025 Review: AI, Platforms, Data, and Deep Technical Lessons Learned](https://www.wix.engineering/post/the-wix-engineering-2025-review-ai-platforms-data-and-deep-technical-lessons-learned)
- [Rebuilding the Online Feature Store with Kafka and Flink](https://www.kai-waehner.de/blog/2025/09/15/online-feature-store-for-ai-and-machine-learning-with-apache-kafka-and-flink/)
- [kafka consumer proxy](https://www.wix.engineering/post/from-bottleneck-to-breakthrough-how-wix-cut-kafka-costs-by-30-with-a-push-based-consumer-proxy)
- [High-level SDK for Kafka: Greyhound Unleashed](https://www.wix.engineering/post/building-a-high-level-sdk-for-kafka-greyhound-unleashed)
- [video: Before and After: Transforming Wix's Online Feature Store with Apache Flink](https://current.confluent.io/post-conference-videos-2025/before-and-after-transforming-wixs-online-feature-store-with-apache-flink-lnd25)
- [video: Async Excellence: Unlocking Scalability with Kafka - Devoxx Greece 2025](https://www.youtube.com/watch?v=QtTh_Qn2e54)
- [video: Redefining Event-Driven Architecture at Wix: Integration Ignited - DDD Europe 2025](https://2025.dddeurope.com/program/redefining-event-driven-architecture-at-wix-integration-ignited/)
- [video: How to Build 1000 Microservices with Kafka](https://www.youtube.com/watch?v=ZGqHTuA2uII)
- [video: Kafka Based Global Data Mesh at Wix](https://www.youtube.com/watch?v=NHj1fSg7M-M)
- [video: Greyhound Kafka library](https://www.youtube.com/watch?v=Up4HkV_9G-M)
- [video: Migrating 2000 microservices to Multi Cluster Managed Kafka with 0 Downtime](https://www.youtube.com/watch?v=XKbG8a-9NRE)
- [video: Exactly Once Delivery is a Harsh Mistress](https://www.youtube.com/watch?v=XkXSjYvX4Mg)
- [video: GeeCON 2023: Natan Silnitsky - Lessons learned from working with 2000 event-driven microservices](https://www.youtube.com/watch?v=y1rgKdN7Bv4)
- [video: 10 lessons learned from using Kafka in more than 1000 microservices](https://www.youtube.com/watch?v=N7kRAVvgsoM)
- [video: Migrating to Multi Cluster Managed Kafka - Natan Silnitsky](https://www.youtube.com/watch?v=jk6mYUuqOAs)
- [video: Greyhound library at Scala Love in the City](https://www.youtube.com/watch?v=H8blL1gS30k)
- [video: Kafka Based Global Data Mesh At Wix- Natan Silnitsky](https://www.youtube.com/watch?v=3kEueGs1gkc)
- [greyhound library](https://github.com/wix/greyhound)
 
# Web UI for Kafka

- [Redpanda Console Web UI](https://redpanda.com/redpanda-console-kafka-ui)

# spring-kafka
- [Spring Kafka Headers](https://docs.spring.io/spring-kafka/reference/kafka/headers.html)
  
# Moving data
- [9 ways to move data kafka to iceberg](https://blog.streambased.io/p/the-9-ways-to-move-data-kafka-iceberg)
