---
title: DedupBuilder
description: A sub-builder for deduplicating records in a stream based on a group key and an optional dedup value function.
order: 32
---

# DedupBuilder

DedupBuilder is a sub-builder that allows you to deduplicate records in a stream based on a group key and an optional
dedup value function.

## Usage

Initialize the DedupBuilder by calling the `dedup()` method on the `KipesBuilder` instance:

```java
DedupBuilder<String, Integer, String, Integer> dedupBuilder = kipesBuilder.dedup();
```

Set the group key function by calling the `groupBy()` method, which accepts a BiFunction to extract the group key from
the original key and value. You can also provide an optional Serde for the group key:

```java
dedupBuilder = dedupBuilder.groupBy((key, value) -> key);
```

If you want to deduplicate based on a specific subset value of the input record, set the dedup value function using
the `advanceBy()` method:

```java
dedupBuilder = dedupBuilder.advanceBy((key, value) -> value);
```

Call the `emitFirst()` method to get a KipesBuilder instance with the deduplication transformation applied:

```java
kipesBuilder = dedupBuilder.emitFirst();
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
