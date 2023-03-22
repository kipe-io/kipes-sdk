/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.streams.kafka.processors;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * A builder for transforming incoming records into zero or more outgoing records with transformed keys, values, or
 * both. It is not meant to be instantiated directly by clients, but instead accessed through
 * {@link KipesBuilder#transform()}.
 * <p>
 * The intended transformation can be selected by starting with one of the {@code intoXXX(...)} methods followed and
 * finalized by the related {@code asXXX(...)} method.
 * <p>
 * The transformation functions should be specified in {@link BiFunction} format where the first argument is the
 * incoming key and the second argument is the incoming value. The output of the transformation can be either a
 * transformed value, a transformed key, a transformed key-value pair or a list of transformed key-value pairs.
 * <p>
 * Example:
 *
 * <pre>{@code StreamsBuilder builder = new StreamsBuilder();
 * KStream<String, Integer> stream = builder.stream("input-topic");
 * Serde<String> keySerde = Serdes.String();
 * Serde<Integer> valueSerde = Serdes.Integer();
 *
 * TransformBuilder<String, Integer, String, String> transformBuilder =
 *         new TransformBuilder<>(
 *                 builder,
 *                 stream,
 *                 keySerde,
 *                 valueSerde,
 *                 "output-topic"
 *         );
 *
 * transformBuilder
 *         .changeValue((key, value) -> value.toString())
 *         .asValueType(Serdes.String());}
 * </pre>
 * <p>
 * In this example, we first create an instance of StreamsBuilder and then use it to build a stream from an input topic.
 * We also define serializers/deserializers (serdes) for the key and value of the input stream.
 * <p>
 * Next, we create an instance of the TransformBuilder class and pass the StreamsBuilder, the input stream, and the key
 * and value serdes to its constructor.
 * <p>
 * Finally, we use the changeValue method to specify a transformation function that converts the incoming values from
 * Integer to String, and then use the asKipesBuilder method to build and return the topology.
 *
 * @param <K>  the source stream's key type.
 * @param <V>  the source stream's value type.
 * @param <KR> the target stream's key type.
 * @param <VR> the target stream's value type.
 */
// TODO introduce sub builders to improve encapsulation of the individual cases
// It's currently to easy to combine wrong intoXXX and asXXX variants.

// TODO introduce asXXX variants to simplify same key/value type transformations
// Transform not only supports changing types but also changing quantities. In
// latter cases it would be nice to have asXXX methods which do not ask for new
// serdes.
// NOTE: makes sense only if we got sub builders (see to do above)

// TODO add tests
public class TransformBuilder<K,V, KR,VR> extends AbstractTopologyPartBuilder<K, V>{

	private BiFunction<K,V, Iterable<VR>> transformValueFunction;
	private BiFunction<K,V, Iterable<KR>> transformKeyFunction;
	private BiFunction<K,V, Iterable<KeyValue<KR,VR>>> transformKeyValueFunction;

	/**
	 * @param streamsBuilder instance of {@link StreamsBuilder} to build the topology.
	 * @param stream         incoming stream to transform.
	 * @param keySerde       key serde for the incoming stream.
	 * @param valueSerde     value serde for the incoming stream.
	 * @param topicsBaseName base name for the output topics.
	 */
	TransformBuilder(
			StreamsBuilder streamsBuilder,
			KStream<K, V> stream,
			Serde<K> keySerde,
			Serde<V> valueSerde,
			String topicsBaseName)
	{
		super(streamsBuilder, stream, keySerde, valueSerde, topicsBaseName);
	}

	// ------------------------------------------------------------------------
	// transform values
	// ------------------------------------------------------------------------

    /**
     * Transform the value of the incoming stream.
     *
     * @param transformValueFunction function to transform the value.
     * @return this instance.
     */
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, K,VR> changeValue(BiFunction<K,V, VR> transformValueFunction) {

		this.transformValueFunction = (key, value) -> {
			VR result = transformValueFunction.apply(key, value);
			if(result == null) {
				return Collections.emptyList();
			} else {
				return Arrays.asList(transformValueFunction.apply(key, value));
			}
		};

		return (TransformBuilder<K,V, K,VR>)this;
	}

    /**
     * Transform the value of the incoming stream.
     *
     * @param transformValueFunction function to transform the value.
     * @return this instance.
     */
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, K,VR> newValues(BiFunction<K,V, Iterable<VR>> transformValueFunction) {
		this.transformValueFunction = transformValueFunction;

		return (TransformBuilder<K,V, K,VR>)this;
	}

    /**
     * Returns a new {@link KipesBuilder} with the specified value serde.
     * <p>
     * The returned topology builder applies the {@link #transformValueFunction} to the input stream,
     * <p>
     * and maps the resulting iterable to a new stream.
     *
     * @param resultValueSerde the serde to use for the new stream's values.
     * @return a new {@link KipesBuilder} with the specified value serde.
     * @throws NullPointerException if {@link #transformValueFunction} is null.
     */
        public KipesBuilder<K,VR> asValueType(Serde<VR> resultValueSerde) {
            Objects.requireNonNull(this.transformValueFunction, "transformValueFunction");

            return createKipesBuilder(
                    this.stream
                    .flatMapValues(
                            (key, value) ->
                                this.transformValueFunction.apply(key,value)),
                    this.keySerde,
                    resultValueSerde);
        }

	// ------------------------------------------------------------------------
	// transform keys
	// ------------------------------------------------------------------------

    /**
     * Change the key type of the incoming record.
     *
     * <p>This method is the first step to use before calling {@link #asKeyType(Serde)}.
     *
     * @param transformKeyFunction a function that takes the current key and value and returns a new key.
     * @return this TransformBuilder.
     */
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, KR,V> changeKey(BiFunction<K,V, KR> transformKeyFunction) {
		this.transformKeyFunction = (key, value) -> Arrays.asList(transformKeyFunction.apply(key, value));

		return (TransformBuilder<K,V, KR,V>)this;
	}


    /**
     * Create new keys based on the existing key and value.
     *
     * <p>This method is the first step to use before calling {@link #asKeyType(Serde)}.
     *
     * @param transformKeyFunction a function that takes the current key and value and returns a new keys.
     * @return this TransformBuilder.
     */
	@SuppressWarnings("unchecked")
	public TransformBuilder<K,V, KR,V> newKeys(BiFunction<K,V, Iterable<KR>> transformKeyFunction) {
		this.transformKeyFunction = transformKeyFunction;

		return (TransformBuilder<K,V, KR,V>)this;
	}

    /**
     * This method is used to finalize the key transformation step and create a new {@link KipesBuilder} with the
     * transformed key type, KR.
     * <p>This method should be called after one of the key transformation methods, {@link #changeKey(BiFunction)} or
     * {@link #newKeys(BiFunction)}, have been called.
     *
     * @param resultKeySerde the {@link Serde} to be used for the transformed key type, KR.
     * @return a new {@link KipesBuilder} with the transformed key type, KR.
     */
	public KipesBuilder<KR,V> asKeyType(Serde<KR> resultKeySerde) {
		Objects.requireNonNull(this.transformKeyFunction, "transformKeyFunction");
		return createKipesBuilder(
				this.stream
				.flatMap(
						(key, value) -> {
							List<KeyValue<KR,V>> keyValues = new LinkedList<>();

							this.transformKeyFunction.apply(key,value)
							.forEach(resultKey -> keyValues.add(new KeyValue<>(resultKey, value)));

							return keyValues;
						}),
				resultKeySerde,
				this.valueSerde);
	}

	// ------------------------------------------------------------------------
	// transform keys and values
	// ------------------------------------------------------------------------

    /**
     * Transform the key and value of the input stream using the provided {@link BiFunction}.
     * <p>This method is the first step to use before calling {@link #asKeyValueType(Serde, Serde)}.
     *
     * @param transformKeyValueFunction {@link BiFunction} to transform the key and value of the input stream.
     * @return The current {@link TransformBuilder} instance.
     */
	public TransformBuilder<K,V, KR,VR> newKeyValues(BiFunction<K,V, Iterable<KeyValue<KR,VR>>> transformKeyValueFunction) {
		this.transformKeyValueFunction = transformKeyValueFunction;

		return this;
	}

    /**
     * Transforms the key and value of the input stream using the provided {@link BiFunction} and creates a new
     * {@link KipesBuilder} with the transformed key and value serdes.
     * <p> This method should be called after {@link #newKeyValues(BiFunction)}.
     *
     * @param resultKeySerde   the serde of the transformed key.
     * @param resultValueSerde the serde of the transformed value.
     * @return the new {@link KipesBuilder} instance with the transformed key and value serdes.
     */
	public KipesBuilder<KR,VR> asKeyValueType(Serde<KR> resultKeySerde, Serde<VR> resultValueSerde) {
		Objects.requireNonNull(this.transformKeyValueFunction, "transformKeyValueFunction");
		return createKipesBuilder(
				this.stream
				.flatMap(
						(key, value) ->
							this.transformKeyValueFunction.apply(key,value)),
				resultKeySerde,
				resultValueSerde);

	}

}
