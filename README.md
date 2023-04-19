# Kipes SDK

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Build Status](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml/badge.svg)](https://github.com/kipe-io/kipes-sdk/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/kipe-io/kipes-sdk/branch/main/graph/badge.svg?token=YOURTOKEN)](https://codecov.io/gh/kipe-io/kipes-sdk)
[![Maven Central](https://img.shields.io/maven-central/v/io.kipe/kipes-sdk.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.kipe%22%20AND%20a:%22kipes-sdk%22)
[![Javadocs](https://www.javadoc.io/badge/io.kipe/kipes-sdk.svg)](https://www.kipe.io/doc/io.kipe/kipes-sdk)
[![Contributors](https://img.shields.io/github/contributors/kipe-io/kipes-sdk.svg)](https://github.com/kipe-io/kipes-sdk/graphs/contributors)

Kipe.io is a user-friendly wrapper around the Kafka Streams API that simplifies building and managing Kafka stream
topologies. It provides a fluent API for building and chaining various stream operations, making it easier to work with
Kafka Streams.
Table of Contents

<!-- TOC -->
* [Kipes SDK](#kipes-sdk)
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
    * [Testing with `AbstractTopologyTest`](#testing-with-abstracttopologytest)
    * [Testing with `AbstractGenericRecordProcessorTopologyTest`](#testing-with-abstractgenericrecordprocessortopologytest)
  * [Examples](#examples)
    * [Basic Example](#basic-example)
    * [Advanced Example](#advanced-example)
  * [Documentation](#documentation)
  * [Contributing](#contributing)
  * [License](#license)
<!-- TOC -->

## Features

- Simplified stream topology creation with fluent API
- Processors built for common use-cases
- Pre-packaged serializers for JSON, Avro, and Protobuf
- Support for custom serializers
- Stream testing utilities
- And more!

## Requirements

- Java 8 or higher

## Getting Started

Add the Kipe dependency to your project using Maven or Gradle.

### Maven

```xml

<dependency>
	<groupId>io.kipe</groupId>
	<artifactId>kipes-sdk</artifactId>
	<version>0.1-SNAPSHOT</version>
</dependency>
```

### Gradle

```groovy
implementation group: 'io.kipe', name: 'kipes-sdk', version: '0.1-SNAPSHOT'
```

## Usage

### Initialization

To create a `KipesBuilder`, you must first have a Kafka Streams `StreamsBuilder`. Instantiate a new `KipesBuilder` using
the `init()` method, passing in a `StreamsBuilder` object.

```java
StreamsBuilder streamsBuilder=new StreamsBuilder();
        KipesBuilder<K, V> kipesBuilder=KipesBuilder.init(streamsBuilder);
```

### Building Stream Topologies

Specify the input `KStream` and its corresponding `Serdes`, and pass them into the `from()` method:

```java
KStream<String, Integer> inputStream=streamsBuilder.stream("inputTopic");
        kipesBuilder.from(inputStream,Serdes.String(),Serdes.Integer());
```

Chain various operations on the KipesBuilder instance to build your desired stream topology:

```java
kipesBuilder
        .logDebug("Input")
        .filter((key,value)->value>0)
        .logDebug("Filtered")
        .to(outputTopic);
```

## Serializers

Kipe includes pre-packaged serializers for JSON, Avro, and Protobuf. You can also use custom serializers or override
default serializers by passing in a `Serde` to various builder methods that require streams.

### JSON

Use the `JsonSerdeFactory` to obtain `Serde` instances for JSON serialization and deserialization using Jackson:

```java
Serde<MyDataClass> jsonSerde=JsonSerdeFactory.getJsonSerde(MyDataClass.class);
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

### Testing with `AbstractTopologyTest`

`AbstractTopologyTest` is a base class for testing Kipe applications using `TopologyTestDriver`. To create tests for
your builders, follow these steps:

1. Extend `AbstractTopologyTest`.
2. Implement `initTopology()` and `initTestTopics()` to set up the topology and test topics.
3. Create test input and output topics using `TopologyTestContext`.
4. Send and receive messages using `TestInputTopic` and `TestOutputTopic`.

### Testing with `AbstractGenericRecordProcessorTopologyTest`

For topologies processing `GenericRecords`, extend `AbstractGenericRecordProcessorTopologyTest`:

1. Extend `AbstractGenericRecordProcessorTopologyTest`.
2. Override `addGenericRecordProcessor()` to add the specific processor. This abstracts initializing the topology and
   topics.
3. Send `GenericRecords` to the input topic using provided utility methods.

## Examples

### Basic Example

Here is an example of using `KipesBuilder` to create a simple stream topology:

```java
KipesBuilder<String, Integer> kipesBuilder=KipesBuilder.init(streamsBuilder);

// Chain various operations on the KipesBuilder instance
        kipesBuilder
        .from(inputStream,Serdes.String(),Serdes.Integer())
        .logDebug("Input")
        .filter((key,value)->value>0)
        .logDebug("Filtered")
        .to(outputTopic);

// run the stream…
```

### Advanced Example

Here is an example of using `KipesBuilder` and sub-builders to create a more advanced stream topology:

```java
JsonSerdeRegistry serdes=topologyTestContext.getJsonSerdeRegistry();
        StreamsBuilder streamsBuilder=new StreamsBuilder();

// Create the kipe builder
        KipesBuilder<String, GenericRecord> builder=KipesBuilder
        .init(topologyTestContext.getStreamsBuilder())
        .from(
        streamsBuilder
        .stream(
        SOURCE,
        Consumed.with(
        serdes.getSerde(String.class),
        serdes.getSerde(GenericRecord.class)
        )
        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
        ),
        serdes.getSerde(String.class),
        serdes.getSerde(GenericRecord.class)
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
