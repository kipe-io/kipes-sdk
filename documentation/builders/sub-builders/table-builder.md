---
title: TableBuilder
description: A sub-builder for creating tables from a KStream of records.
order: 37
---

# TableBuilder

TableBuilder is a sub-builder in the Kipes SDK designed to create tables for processing Kafka Streams records. It takes
a stream of records and builds a table by storing the records in a KeyValueStore. The table can be further processed or
output to a Kafka topic using the KipesBuilder API.

## Usage

Initialize the TableBuilder by calling the `table()` method on the KipesBuilder instance:

```java
TableBuilder<String> tableBuilder = kipesBuilder.table();
```

Build the table:

```java
KipesBuilder<String, TableRecord<String, GenericRecord>> kipesBuilderWithTable = tableBuilder.build(resultKeySerde, resultValueSerde);
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
