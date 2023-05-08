---
title: KipesBuilder
description: An overview of KipesBuilder and its usage in stream processing tasks.
order: 10
---

# KipesBuilder

KipesBuilder is a powerful, flexible, and extensible component of the Kipes SDK, designed to simplify the process of
building, transforming, and processing Apache Kafka Streams topologies. It provides a convenient and easy-to-use API for
stream processing, including filtering, joining, deduplication, and more.

## Usage

### Initialization

Create a new instance of `KipesBuilder` using the `init()` method and passing a `StreamsBuilder` object:

```java
StreamsBuilder streamsBuilder=new StreamsBuilder();
        KipesBuilder<String, Integer> kipesBuilder=KipesBuilder.init(streamsBuilder);
```

### Configuring a Stream

To configure a stream, use the `from()` method and pass the `KStream` object along with the key and value SerDes if
required:

```java
KStream<String, Integer> inputStream = streamsBuilder.stream("input-topic");
kipesBuilder = kipesBuilder.from(inputStream);
```

### Applying Transformations

You can apply various transformations on the stream, such as `filter()`, `join()`, `dedup()`, and more:

```java
kipesBuilder = kipesBuilder.filter((key,value) -> value != null && value > 10);
```

### Output to a Topic

Finally, to output the results to a topic, use the to() method:

```java
kipesBuilder.to("output-topic");
```
