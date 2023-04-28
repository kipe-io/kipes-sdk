---
title: TransformBuilder
description: A sub-builder for creating transformations for processing Kafka Streams records.
order: 39
---

# TransformBuilder

TransformBuilder is a sub-builder in the Kipes SDK designed to create transformations for processing Kafka Streams
records. It takes a KStream of records and allows the user to transform the keys, values, or both using specified
functions. The transformed records can be further processed or output to a Kafka topic using the KipesBuilder API.

## Usage

Initialize the TransformBuilder by calling the `transform()` method on the KipesBuilder instance:

```java
TransformBuilder<String, String, String, String> transformBuilder = kipesBuilder.transform();
```
Apply the desired transformation functions:

```java
TransformBuilder<String, String, String, Integer> transformedBuilder = transformBuilder.changeValue((key, value) -> Integer.parseInt(value));
```
Build the transformed KStream:

```java
KipesBuilder<String, Integer> kipesBuilderWithTransformedStream = transformedBuilder.asValueType(resultValueSerde);
```
Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
