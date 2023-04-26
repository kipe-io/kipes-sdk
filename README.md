# Kipes SDK

[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Build Status](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml/badge.svg)](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml)
[![Maven Central](https://img.shields.io/maven-central/v/io.kipe/kipes-sdk)](https://search.maven.org/search?q=g:io.kipe%20AND%20a:kipes-sdk)
[![Contributors](https://img.shields.io/github/contributors/kipe-io/kipes-sdk.svg)](https://github.com/kipe-io/kipes-sdk/graphs/contributors)

The Kipes SDK simplifies the implementation of Kafka stream processing applications. The SDK provides a high-level interface to describe stream analytics, eliminates the need for much of the repetitive technical boilerplate code, and provides scaffolding to set up stream processing microservices quickly and structured. 

We built the SDK applying the concept of Linux command pipes, making it easy to pick a specific command for each stream transformation case and forward the results to the next. The SDK commands cover areas like:
- Event and field manipulation
- Event filtering
- Event correlation
- Statistical evaluations
- Event time adjustments

With these dedicated commands, Engineers can directly create complex stream-processing applications in a much more business logic-aligned language. 

**Example**

Story: As a ProductMarketer I want to know how many customers visited a particular Product but didn't purchased it, so that I can identify what are the most visited Products that not get purchased."

```java
	KipesBuilder.init(streamsBuilder)
	  .from(topicShopEvents)
	  .transaction()
	    .groupBy((key, value) -> value.getSessionId())
	    .startswith((key, value) -> value.getType() == ProductVisited)
	    .endswith((key, value) -> value.getType() == NoPurchase)
	    .emit(END)
	  .stats()
	    .groupBy((key, value) -> value.getProductId())
	    .count().as("qtyVisitedButNotBought")
	    .build()
	  .to(topicProductStats);
```

Besides this easy to use stream processing commands the SDK provides specialized test classes so that Engineers can quickly set up unit tests around their stream topologies without connecting to an actual running Kafka cluster. The testbed speeds up development and delivery time and makes testing and understanding complex applications more accessible. 

To further speed up the development of stream-processing microservices, our Kipes SDK comes with dedicated classes and blueprints to scaffold microservices quickly. We support multiple application frameworks like Micronaut or Spring Boot (planned). 

## Table of Contents

<!-- TOC -->
* [Kipes SDK](#kipes-sdk)
  * [Table of Contents](#table-of-contents)
  * [Features](#features)
  * [Requirements](#requirements)
  * [Getting Started](#getting-started)
    * [Maven](#maven)
    * [Gradle](#gradle)
  * [Usage](#usage)
    * [Initialization and Building Stream Topologies](#initialization-and-building-stream-topologies)
  * [GenericRecord](#genericrecord)
  * [Serializers](#serializers)
    * [Default Serdes](#default-serdes)
    * [JSON](#json)
    * [Avro](#avro)
    * [Protobuf](#protobuf)
  * [Testing](#testing)
    * [Testing with AbstractTopologyTest](#testing-with-abstracttopologytest)
      * [Example](#example)
    * [Testing with AbstractGenericRecordProcessorTopologyTest](#testing-with-abstractgenericrecordprocessortopologytest)
  * [Examples](#examples)
    * [Basic Example](#basic-example)
    * [Advanced Example](#advanced-example)
  * [Documentation](#documentation)
  * [Contributing](#contributing)
  * [License](#license)
<!-- TOC -->

## Features

- High-level, multi-faceted stream processing commands in a fluent API
- Out-of-the-box serializers for JSON, Avro, and Protobuf
- Custom serializer support
- Stream testing utilities
- And more!

## Requirements

- Java 11 or higher

## Getting Started

Add the Kipes SDK dependency to your project using Maven or Gradle.

### Maven

```xml
<dependency>
	<groupId>io.kipe</groupId>
	<artifactId>kipes-sdk</artifactId>
	<version>${kipes.version}</version>
</dependency>
```

### Gradle

```groovy
dependencies {
	implementation "io.kipe:kipes-sdk:$kipesVersion"
}
```

## Usage

### Initialization and Building Stream Topologies

Follow these steps to create a KipesBuilder instance, define an input KStream, and build the stream topology by chaining
operations:

```java
// 1. Create a KipesBuilder instance with a StreamsBuilder object
StreamsBuilder streamsBuilder = new StreamsBuilder();
KipesBuilder<K, V> kipesBuilder = KipesBuilder.init(streamsBuilder);

// 2. Define the input KStream and pass it to the from() method
KStream<String, Integer> inputStream = streamsBuilder.stream("inputTopic");
kipesBuilder.from(inputStream, Serdes.String(), Serdes.Integer());

// 3. Chain operations on the KipesBuilder instance to build the stream topology
kipesBuilder
    .logDebug("Input")
    .filter((key, value) -> value > 0)
    .logDebug("Filtered")
    .to(outputTopic);
```

## GenericRecord

`GenericRecord` is a flexible data representation in the Kipes SDK for storing and manipulating records with various
fields. It allows reading and writing data without generating code based on a specific schema, making it ideal for
evolving data structures or handling data with different field combinations. The Kipes SDK uses `GenericRecord` in
builder
classes such as `EvalBuilder`, `BinBuilder`, `StatsBuilder`, and `TableBuilder`.

Create a `GenericRecord` instance and set field values using the fluent interface or the `set()` method:

```java
GenericRecord record=GenericRecord.create()
    .with("sensorId","S001")
    .with("timestamp",1628493021L)
    .with("temperature",25.6);

record.set("newField": "value");
```

Retrieve field values with `get(fieldName)` and perform advanced operations using other `GenericRecord` methods.

## Serializers

The Kipes SDK comes with pre-packaged serializers for JSON, Avro, and Protobuf. To use custom serializers or override
default serializers, provide a Serde to the builder methods that require streams.

### Default Serdes

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app-id");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

StreamsConfig config = new StreamsConfig(props);
```

### JSON

For JSON serialization and deserialization using Jackson, obtain Serde instances through the JsonSerdeFactory:

```java
Serde<MyDataClass> jsonSerde = JsonSerdeFactory.getJsonSerde(MyDataClass.class);
```

### Avro

For Avro serialization and deserialization using Confluent classes, obtain Serde instances through the AvroSerdeFactory.
Here are some options:

```java
Serde<FooEvent> serde = AvroSerdeFactory.createSpecificAvroSerde(SCHEMA_REGISTRY_URL_CONFIG,false);

GenericAvroSerde serde = AvroSerdeFactory.createGenericAvroSerde(SCHEMA_REGISTRY_URL_CONFIG,false);

PrimitiveAvroSerde<Integer> serde = AvroSerdeFactory.createPrimitiveAvroSerde(SCHEMA_REGISTRY_URL_CONFIG,false);
```

### Protobuf

For Protobuf serialization and deserialization using Confluent classes, obtain Serde instances through the
ProtobufSerdeFactory. Here's an option:

```java
KafkaProtobufSerde<Message> protoSerde=ProtobufSerdeFactory.createProtoSerde(SCHEMA_REGISTRY_URL_CONFIG,false);
```

## Testing

Kipes SDK provides testing support for Kipe topologies through two base classes:

- `AbstractTopologyTest`
- `AbstractGenericRecordProcessorTopologyTest`

These classes utilize `TopologyTestDriver` to test Kipe applications without a running Kafka cluster.

To configure topology-specific properties, pass a map of properties into the `super()` method in the constructor of your
test class:

### Testing with AbstractTopologyTest

`AbstractTopologyTest` is a base class for testing Kipe applications using `TopologyTestDriver`. To create tests for
your builders, follow these steps:

1. Extend `AbstractTopologyTest`.
2. Implement `initTopology()` and `initTestTopics()` to set up the topology and test topics.
3. Create test input and output topics using `TopologyTestContext`.
4. Send and receive messages using `TestInputTopic` and `TestOutputTopic`.

#### Example

In this example, we will create a test for the simple topology from the "Initialization and Building Stream Topologies"
section.

For example, in the SimpleTopologyTest class, you can pass an empty map:``

First, extend `AbstractTopologyTest` and implement the required methods:

```java
import io.kipe.sdk.testing.AbstractTopologyTest;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

class SimpleTopologyTest extends AbstractTopologyTest {
    private final String INPUT_TOPIC = "inputTopic";
    private final String OUTPUT_TOPIC = "outputTopic";

    private TestInputTopic<String, Integer> inputTopic;
    private TestOutputTopic<String, Integer> outputTopic;

    public SimpleTopologyTest() {
        super(Map.of());
    }

    @Override
    protected void initTopology(TopologyTestContext topologyTestContext) {
        KipesBuilder<?, ?> kipesBuilder = KipesBuilder.init(topologyTestContext.getStreamsBuilder());

        kipesBuilder
                .from(topologyTestContext.createKStream(INPUT_TOPIC, Serdes.String(), Serdes.Integer()), Serdes.String(), Serdes.Integer())
                .logDebug("Input")
                .filter((key, value) -> value > 1)
                .logDebug("Filtered")
                .to(OUTPUT_TOPIC);
    }

    @Override
    protected void initTestTopics(TopologyTestContext topologyTestContext) {
        this.inputTopic = topologyTestContext.createTestInputTopic(INPUT_TOPIC, Serdes.String(), Serdes.Integer());
        this.outputTopic = topologyTestContext.createTestOutputTopic(OUTPUT_TOPIC, Serdes.String(), Serdes.Integer());
    }
}
```

Now, add a test method to send input records and verify the output records:

```java
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimpleTopologyTest extends AbstractTopologyTest {

    // ...

    @Test
    void testFilterPositiveValues() {
        // Send 5 input records to the input topic
        inputTopic.pipeInput("key1", 5);
        inputTopic.pipeInput("key2", -3);
        inputTopic.pipeInput("key3", 7);
        inputTopic.pipeInput("key4", 0);
        inputTopic.pipeInput("key5", 2);

        // Get 3 records back after the filter
        assertEquals(3, this.outputTopic.getQueueSize());
    }
}
```

### Testing with AbstractGenericRecordProcessorTopologyTest

For topologies processing `GenericRecords`, extend `AbstractGenericRecordProcessorTopologyTest`:

1. Extend `AbstractGenericRecordProcessorTopologyTest`.
2. Override `addGenericRecordProcessor()` to add the specific processor. This abstracts initializing the topology and
   topics.
3. Send `GenericRecords` to the input topic using provided utility methods.

## Examples

### Basic Example

Here's a simple example of using `KipesBuilder` to create a stream topology:

```java
KipesBuilder<String, Integer> kipesBuilder = KipesBuilder.init(streamsBuilder);

// Chain various operations on the KipesBuilder instance
kipesBuilder
    .from(inputStream, Serdes.String(), Serdes.Integer())
    .logDebug("Input")
    .filter((key, value) -> value > 0)
    .logDebug("Filtered")
    .to(outputTopic);

// run the stream…
```

### Advanced Example

This example demonstrates using `KipesBuilder` and sub-builders to create a more complex stream topology:

```java
KipesBuilder<String, GenericRecord> builder = KipesBuilder
    .init(streamsBuilder)
    .from(inputStream)
    .withTopicsBaseName(SOURCE);

builder
    .bin()
    .field("input")
    .span(0.1)
    .build()
    .to(TARGET);
```

## Documentation

TODO: Add instructions on how to generate project documentation, e.g., with GitHub Pages or another documentation tool.

## Contributing

Contributions are welcome! Please read
the [contributing.md](https://github.com/kipe-io/kipes-sdk/blob/main/documentation/development/contributing.md) file for
guidelines on how to contribute to this project.

## License

This project is licensed under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0).
