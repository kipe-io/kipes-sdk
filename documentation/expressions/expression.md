---
title: Expression
description: A utility class for applying custom functions to update fields in GenericRecord instances.
order: 40
---

# Expression

Expression is a utility class in the Kipes SDK designed for applying custom functions to update fields in GenericRecord
instances. It takes a key and a GenericRecord instance, applies the provided function, and updates the specified field
in the record.

## Usage

Create an Expression instance with the desired field name and value function:

```java
BiFunction<String, GenericRecord, String> valueFunction = (key, record) -> key + ":" + record.get("field");
Expression<String, GenericRecord> expression = new Expression<>("newField", valueFunction);
```

Update the field in the GenericRecord instance with the new value:

```java
expression.update(key, genericRecord);
```
