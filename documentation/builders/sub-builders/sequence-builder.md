---
title: SequenceBuilder
description: A sub-builder for grouping records based on a custom function, defining the size of the sequence, and applying a custom aggregation function to the sequence.
order: 35
---

# SequenceBuilder

SequenceBuilder is a powerful and extensible component of the Kipes SDK, designed to simplify the process of creating
and managing sequences of records in Kafka Streams. It provides a convenient and easy-to-use API for grouping records
and applying custom aggregation logic based on the sequence.

## Usage

Initialize the SequenceBuilder by calling the `sequence()` method on the KipesBuilder instance:

```java
SequenceBuilder<String, V, GK, VR> sequenceBuilder = kipesBuilder.sequence();
```

Configure the grouping function and the group key Serde (optional):

```java
sequenceBuilder = sequenceBuilder.groupBy((k, v) -> /* your custom group key function */, groupKeySerde);
```

Define the sequence size:

```java
sequenceBuilder = sequenceBuilder.size(sequenceSize);
```

Define the aggregation function, the value class, and the result value Serde (optional):

```java
kipesBuilder = sequenceBuilder.as((gk, values) -> /* your custom aggregation logic */, valueClass, resultValueSerde);
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
