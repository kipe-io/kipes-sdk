---
title: Sub-builders
description: Overview of the available sub-builders in KipesBuilder.
order: 30
---

# Sub-builders

Once you have a KipesBuilder stream, you can chain methods to construct sub-builders.

Sub-builders are specialized components within the KipesBuilder that offer specific functionality for various stream
processing tasks. They can be used in conjunction with the KipesBuilder to create more complex topologies. Here are some
of the available sub-builders:

1. [DedupBuilder](sub-builders/dedup-builder.md) - A sub-builder that allows you to deduplicate records in a stream
   based on a group key and an optional dedup value function.
2. [BinBuilder](sub-builders/bin-builder.md) - A sub-builder that allows you to perform binning on a specific field
   of `GenericRecord` instances in a stream.
3. [EvalBuilder](sub-builders/eval-builder.md) - A sub-builder that allows you to evaluate and update fields in a
   GenericRecord instance within a stream.
4. [JoinBuilder](sub-builders/join-builder.md) - A sub-builder that allows you to join two Kafka Streams using a common
   key and a ValueJoiner.
5. [SequenceBuilder](sub-builders/sequence-builder.md) - A sub-builder that allows you to group records based on a
   custom function, define the size of the sequence, and apply a custom aggregation function to the sequence.
6. [StatsBuilder](sub-builders/stats-builder.md) - A sub-builder used to define groups based on specific fields and
   apply aggregation functions on the grouped records.
7. [TableBuilder](sub-builders/table-builder.md) - A sub-builder used to create tables from a KStream of records.
8. [TransactionBuilder](sub-builders/transaction-builder.md) - A sub-builder used to create transactions from a KStream
   of records.
9. [TransformBuilder](sub-builders/transform-builder.md) - A sub-builder used to create transformations for processing
   Kafka Streams records.
