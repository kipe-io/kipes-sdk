---
title: AbstractTopologyPartBuilder
description: A utility class for creating custom sub-builders to process Kafka Streams records.
order: 20
---

# AbstractTopologyPartBuilder

AbstractTopologyPartBuilder is an abstract utility class in the Kipes SDK that serves as the base for creating custom
sub-builders for processing Kafka Streams records. It manages the common properties and methods required for creating
and extending sub-builders.

## Usage

Extend the AbstractTopologyPartBuilder class and implement a custom constructor:

```java
public class CustomSubBuilder<K, V> extends AbstractTopologyPartBuilder<K, V> {

    CustomSubBuilder(StreamsBuilder streamsBuilder,
                     KStream<K, V> stream,
                     Serde<K> keySerde,
                     Serde<V> valueSerde,
                     String topicsBaseName) {
        super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
    }
}
```
Implement custom methods for the sub-builder and use the provided utility methods for creating KipesBuilder instances:

```java
public KipesBuilder<K, V> customTransform() {
    KStream<K, V> transformedStream = this.stream.map(/* Your custom transformation */);
    return createKipesBuilder(transformedStream);
}
```

Create an instance of the custom sub-builder:

```java
CustomSubBuilder<String, GenericRecord> customSubBuilder = new CustomSubBuilder<>(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
```

Apply the custom transformation and continue with the KipesBuilder API:

```java
KipesBuilder<String, GenericRecord> kipesBuilder = customSubBuilder.customTransform();
```
