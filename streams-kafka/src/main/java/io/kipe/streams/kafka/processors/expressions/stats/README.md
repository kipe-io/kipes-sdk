SDK Constructs
--------------

### Max

**Description**: The `Max` class computes the maximum value for a specified field within a dataset.

#### Fields

| Field Name     | Field Type | Description                                               | Renaming Procedure                                        |
|----------------|------------|-----------------------------------------------------------|-----------------------------------------------------------|
| fieldNameToMax | UserField  | The input field name for which to find the maximum value. | N/A                                                       |
| max            | SDKField   | The maximum value of the specified field.                 | Use the `Max.max(String, String)` method to rename field. |

#### Usage

To create a new `Max` instance with the default output field name:

```java
Max maxInstance=Max.max("fieldNameToMax");
```

To create a new `Max` instance with a custom output field name:

```java 
Max maxInstance=Max.max("fieldNameToMax","customOutputFieldName");`
```

#### Notes

The `Max` class extends the `StatsExpression` class and computes the maximum value for a specified field within a
dataset. Users can create a new `Max` instance with a custom output field name to avoid conflicts with other SDK
constructs or user-defined fields. The `fieldNameToMax` parameter is a UserField that specifies which field the maximum
value should be calculated for.

### Min

**Description**: The `Min` class computes the minimum value for a specified field within a dataset.

#### Fields

| Field Name     | Field Type | Description                                               | Renaming Procedure                                        |
|----------------|------------|-----------------------------------------------------------|-----------------------------------------------------------|
| fieldNameToMin | UserField  | The input field name for which to find the minimum value. | N/A                                                       |
| min            | SDKField   | The minimum value of the specified field.                 | Use the `Min.min(String, String)` method to rename field. |

#### Usage

To create a new `Min` instance with the default output field name:

```java
Min minInstance = Min.min("fieldNameToMin");
```

To create a new `Min` instance with a custom output field name:

```java
Min minInstance = Min.min("fieldNameToMin", "customOutputFieldName");
```

#### Notes

The `Min` class extends the `StatsExpression` class and computes the minimum value for a specified field within a
dataset. Users can create a new `Min` instance with a custom output field name to avoid conflicts with other SDK
constructs or user-defined fields. The `fieldNameToMin` parameter is a UserField that specifies which field the minimum
value should be calculated for.