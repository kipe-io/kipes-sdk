---
title: StatsExpression
description: An abstract utility class for applying custom statistical functions to update fields in GenericRecord instances.
order: 41
---

# StatsExpression

StatsExpression is an abstract utility class in the Kipes SDK designed for applying custom statistical functions to
update fields in GenericRecord instances. It takes a group key, a GenericRecord instance, and an aggregate record,
applies the provided stats function, and updates the specified field in the aggregate record.

## Usage

Extend the StatsExpression class and implement a custom constructor and stats function:

```java
public class CustomStatsExpression extends StatsExpression {

    public CustomStatsExpression(String defaultFieldName) {
        super(defaultFieldName);
        this.statsFunction = (groupKey, value, aggregate) -> {
            // Custom stats function implementation
        };
    }
}
```

Create an instance of the custom StatsExpression class:

```java
CustomStatsExpression customStatsExpression = new CustomStatsExpression("customField");
```
Update the field in the aggregate GenericRecord instance with the new value:

```java
customStatsExpression.update(groupKey, value, aggregate);
```
