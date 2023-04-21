# Kipes SDK

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Build Status](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml/badge.svg)](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml)
[![Maven Central](https://img.shields.io/maven-central/v/io.kipe/kipes-sdk)](https://search.maven.org/search?q=g:%22io.kipe%22%20AND%20a:%22kipes-sdk%22)
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
  Kipes.with(StreamBuilder)
  .from(topicShopEvents)
  .transaction()
    .groupBy("sessionId")
    .startswith(type == ProductVisited)
    .endswith(type == NoPurchase)
    .emitEvents()
  .filter(type == ProductVisited)
  .stats()
    .groupBy("productId")
    .count().as("qtyVisitedButNotBought")
    .build()
  .to(topicProductStats)
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
    * [Initialization](#initialization)
    * [Building Stream Topologies](#building-stream-topologies)
  * [Serializers](#serializers)
    * [JSON](#json)
    * [Avro](#avro)
    * [Protobuf](#protobuf)
  * [Testing](#testing)
    * [Testing with AbstractTopologyTest](#testing-with-abstracttopologytest)
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
- Pre-packaged serializers for JSON, Avro, and Protobuf
- Support for custom serializers
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

### Initialization

To create a `KipesBuilder`, you must first have a Kafka Streams `StreamsBuilder`. Instantiate a new `KipesBuilder` using the `init()` method, passing in a `StreamsBuilder` object.

```java
StreamsBuilder streamsBuilder = new StreamsBuilder();
KipesBuilder<K, V> kipesBuilder = KipesBuilder.init(streamsBuilder);
```

### Building Stream Topologies

Specify the input `KStream` and its corresponding `Serdes`, and pass them into the `from()` method:

```java
KStream<String, Integer> inputStream = streamsBuilder.stream("inputTopic");
kipesBuilder.from(inputStream, Serdes.String(), Serdes.Integer());
```

Chain various operations on the KipesBuilder instance to build your desired stream topology:

```java
kipesBuilder
    .logDebug("Input")
    .filter((key, value) -> value > 0)>value>0)
    .logDebug("Filtered"))
    .to(outputTopic);
```
## GenericRecord

*add info*

## Serializers

Kipe includes pre-packaged serializers for JSON, Avro, and Protobuf. You can also use custom serializers or override
default serializers by passing in a `Serde` to various builder methods that require streams.

### JSON

Use the `JsonSerdeFactory` to obtain `Serde` instances for JSON serialization and deserialization using Jackson:

```java
Serde<MyDataClass> jsonSerde = JsonSerdeFactory.getJsonSerde(MyDataClass.class);
```

### Avro

Use the `AvroSerdeFactory` to obtain `Serde` instances for Avro serialization and deserialization using Confluent
classes:

```java
// TODO: Add example of using Avro serialization
```

### Protobuf

Use the `ProtobufSerdeFactory` to obtain `Serde` instances for Protobuf serialization and deserialization using
Confluent classes:

```java
// TODO: Add example of using Protobuf serialization
```

## Testing

The Kipe SDK offers testing support for Kipe topologies through two base classes:

- `AbstractTopologyTest`
- `AbstractGenericRecordProcessorTopologyTest`

These classes utilize `TopologyTestDriver` to test Kipe applications without a running Kafka cluster.

### Testing with AbstractTopologyTest

`AbstractTopologyTest` is a base class for testing Kipe applications using `TopologyTestDriver`. To create tests for
your builders, follow these steps:

1. Extend `AbstractTopologyTest`.
2. Implement `initTopology()` and `initTestTopics()` to set up the topology and test topics.
3. Create test input and output topics using `TopologyTestContext`.
4. Send and receive messages using `TestInputTopic` and `TestOutputTopic`.

### Testing with AbstractGenericRecordProcessorTopologyTest

For topologies processing `GenericRecords`, extend `AbstractGenericRecordProcessorTopologyTest`:

1. Extend `AbstractGenericRecordProcessorTopologyTest`.
2. Override `addGenericRecordProcessor()` to add the specific processor. This abstracts initializing the topology and
   topics.
3. Send `GenericRecords` to the input topic using provided utility methods.

## Examples

### Basic Example

Here is an example of using `KipesBuilder` to create a simple stream topology:

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

Here is an example of using `KipesBuilder` and sub-builders to create a more advanced stream topology:

```java
JsonSerdeRegistry serdes = topologyTestContext.getJsonSerdeRegistry();
StreamsBuilder streamsBuilder = new StreamsBuilder();

// Create the kipe builder
KipesBuilder<String, GenericRecord> builder = KipesBuilder
    .init(topologyTestContext.getStreamsBuilder())
    .from(
        streamsBuilder
            .stream(
                SOURCE,
                Consumed.with(
                    JsonSerdeFactory.getJsonSerde(String.class),
                    JsonSerdeFactory.getJsonSerde(GenericRecord.class)
                )
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
            ),
        JsonSerdeFactory.getJsonSerde(String.class),
        JsonSerdeFactory.getJsonSerde(GenericRecord.class)
    )
    .withTopicsBaseName(SOURCE);

builder
    // call the sub-builder
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

This project is licensed under
the [GNU General Public License v3.0](https://github.com/kipe-io/kipes-sdk/blob/main/LICENSE).
