---
title: TransactionBuilder
description: A sub-builder for creating transactions from a KStream of records.
order: 38
---

# TransactionBuilder

TransactionBuilder is a sub-builder in the Kipes SDK designed to process Kafka Streams records as transactions. It
groups records by a given key, and emits transaction records based on the specified start, end, and emit conditions.

## Usage

Initialize the TransactionBuilder by calling the `transaction()` method on the KipesBuilder instance:

```java
TransactionBuilder<String, GenericRecord, String> transactionBuilder = kipesBuilder.transaction();
```
Configure the transaction conditions (group by, starts with, ends with, emit type):

```java
transactionBuilder.groupBy((key, value) -> key)
   .startsWith((key, value) -> value.startsWithCondition())
   .endsWith((key, value) -> value.endsWithCondition())
   .emit(EmitType.ALL);
```
Build the transactions:

```java
KipesBuilder<String, TransactionRecord<String, GenericRecord>> kipesBuilderWithTransactions = transactionBuilder.as();
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
