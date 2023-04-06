---
label: ADR GenericRecord Field Names
authors:
 - name: Jens Günther
   link: https://github.com/jens-guenther
 - name: John Gerassimou
   link: https://github.com/yianni
order: 10
---

# ADR: GenericRecord Field Names

## Context

GenericRecords field names are not scoped or in any case public or private; 
everyone can define and access them. User-defined fields and SDK-defined
fields coexist.  
  
That means that there's a possibility that an user and a SDK fields clash in a
sense that either users or the SDK overwrite fields defined by the other just 
because both use the same field name.  
  
This ADR defines a field name strategy for all constructs of the Kipes SDK so 
that
- users are aware of the used fields of the SDK
- SDK developers have a guideline to name fields they need to establish

## Decision

### Field Name Strategy

We define three field types and the format the field names must adhere to:

| Field Type | Description | Field Name RegExpr |
| --- | --- | --- |
| UserField     | These are all the fields the User defines. | `UserFieldName := [A-Za-z0-9][A-Za-z0-9.-_]*` |
| SDKField      | Fields established in the context of a specific SDK construct and intended for further processing by the user. | `SDKFieldName := [A-Za-z0-9][A-Za-z0-9.-_]*` |
| InternalField | Fields established in the context of a specific SDK construct and intended for internal processing at the related construct. InternalFields may be accessed by other SDK constructs or the user. | `InternalFieldName := _[A-Za-z0-9.-_]*` |

Furthermore, InternalFields must reference their host construct in the fieldName
so that multiple SDK constructs can use the same internal field:

`InternalFieldName := _{constructName}_{fieldNamePart}`

The `constructName` might be further sub-constructed as needed by the SDK
construct. Typically, the `constructName` equals the `SDKFieldName`

### Renaming of SDKFields

Users must be able to rename SDKFields. A typical use-case is that the same SDK
construct is used multiple times.  
If the user renames a SDKField then that new name must be used to build the
`constructName` of related InternalFields.

### Examples

=== UserField & SDKField
`userId`  
`created_at`  
`1-first-field`  

=== InternalField
`_avg_sum`  
`_avg_count`  
`_ema_listOfElements`  

### Code Documentation

Each SDK construct that introduces fields into a GenericRecord must document
these at the class javadoc.  
  
The documentation must include:
- the field name
- the field type
- the semantics of the field
- in case of SDKFields: how the user can rename the field

## Consequences

### `io.kipe.streams.kafka.processors.StatsExpression`

- we introduced a method `protected String createInternalFieldName(String fieldNamePart)`
