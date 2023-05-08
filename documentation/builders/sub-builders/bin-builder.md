---
title: BinBuilder
description: A sub-builder for performing binning on a specific field of `GenericRecord` instances in a stream.
order: 31
---

# BinBuilder

BinBuilder is a sub-builder that allows you to perform binning on a specific field of `GenericRecord` instances in a
stream. Binning is the process of transforming continuous numerical data into discrete bins or categories. This can be
useful for data analysis, visualization, or preprocessing.

## Usage

Initialize the BinBuilder by calling the `bin()` method on the KipesBuilder instance:

```java
BinBuilder<String> binBuilder = kipesBuilder.bin();
```

Set the field name of the GenericRecord you want to bin by calling the `field()` method:

```java
binBuilder = binBuilder.field("fieldName");
```

Define the span of the bins using the `span()` method:

```java
binBuilder = binBuilder.span(5.0);
```

Optionally, you can set a new field name for the binned values using the `newField()` method. If not provided, the
original field will be replaced with the binned values:

```java
binBuilder = binBuilder.newField("binnedFieldName");
```

Call the `build()` method to get a `KipesBuilder` instance with the binning transformation applied:

```java
kipesBuilder = binBuilder.build();
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
