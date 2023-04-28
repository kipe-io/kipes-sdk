---
title: EvalBuilder
description: A sub-builder for evaluating and updating fields in a GenericRecord instance within a stream.
order: 33
---

# EvalBuilder

EvalBuilder is a sub-builder that allows you to evaluate and update fields in a GenericRecord instance within a stream.
This can be useful for preprocessing, data transformation, or feature extraction.

## Usage

Initialize the EvalBuilder by calling the `eval()` method on the KipesBuilder instance:

```java
EvalBuilder<String> evalBuilder = kipesBuilder.eval();
```

Define the field name to update and provide a BiFunction that takes the key and GenericRecord as input and returns the
new value for the field. Use the `with()` method to add the expression:

```java
evalBuilder = evalBuilder.with("newFieldName", (key, value) -> /* your custom transformation logic */);
```

You can chain multiple `with()` calls to add more expressions to update multiple fields:

```java
evalBuilder = evalBuilder
    .with("field1", (key, value) -> /* custom logic for field1 */)
    .with("field2", (key, value) -> /* custom logic for field2 */);
```

Call the `build()` method to get a KipesBuilder instance with the field updates applied:

```java
kipesBuilder = evalBuilder.build();
```

Continue with the KipesBuilder API to apply further transformations or output the results to a topic.
