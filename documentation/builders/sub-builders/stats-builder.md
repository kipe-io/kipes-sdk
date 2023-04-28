---
title: StatsBuilder
description: A sub-builder for defining groups based on specific fields and applying aggregation functions on the grouped records.
order: 36
---

# StatsBuilder

StatsBuilder is a sub-builder of the Kipes SDK designed to facilitate the process of aggregating and calculating
statistics on Kafka Streams. It allows you to group records based on a set of fields, apply multiple aggregation
expressions, and store the aggregated results in a KTable.

## Usage

Initialize the StatsBuilder by calling the `stats()` method on the KipesBuilder instance:

```java
StatsBuilder<String> statsBuilder = kipesBuilder.stats();
```
Configure the grouping function:

```java
statsBuilder = statsBuilder.groupBy("field1", "field2");
```

Add aggregation expressions:

```java
statsBuilder = statsBuilder.with(expression1).as("resultField1");
statsBuilder = statsBuilder.with(expression2).as("resultField2");
```

Create a KTable with the aggregated results:

```java
KTable<String, GenericRecord> kTable = statsBuilder.asKTable();
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
