---
title: JoinBuilder
description: A sub-builder for joining two Kafka Streams using a common key and a ValueJoiner.
order: 34
---

# JoinBuilder

JoinBuilder is a powerful, flexible, and extensible component of the Kipes SDK, designed to simplify the process of
joining two Kafka Streams based on a common key. It provides a convenient and easy-to-use API for performing windowed
stream joins.

## Usage

Initialize the JoinBuilder by calling the `join()` method on the KipesBuilder instance:

```java
JoinBuilder<String, V, OV, VR> joinBuilder = kipesBuilder.join(otherStream, otherValueSerde);
```

Configure the join window size before and after the join point, and the retention period:

```java
joinBuilder = joinBuilder
.withWindowSizeBefore(Duration.ofMinutes(1))
.withWindowSizeAfter(Duration.ofMinutes(1))
.withRetentionPeriod(Duration.ofHours(1));
```

Define the ValueJoiner to merge the values of the joined records, and provide a Serde for the resulting value type (
optional):

```java
kipesBuilder = joinBuilder.as((v, ov) -> /* your custom merging logic */, resultValueSerde);
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
