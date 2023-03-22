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
package io.kipe.streams.kafka.factories;

/**
 * Abstract base class for creating specific processor factories.
 */
public abstract class AbstractProcessorFactory extends AbstractStreamFactory {

	/**
	 * Returns an empty array of topic names.
	 *
	 * @return empty array of topic names.
	 */
	@Override
	protected String[] getTopicNames() {
		return new String[] {};
	}

	/**
	 * Invokes the initProcessors() method after the object is constructed.
	 *
	 * @throws Exception If an error occurs during processor initialization.
	 */
	@Override
	protected void doPostConstruct() throws Exception {
		initProcessors();
	}

	/**
	 * Abstract method for initializing processors in subclasses.
	 *
	 * @throws Exception If an error occurs during processor initialization.
	 */
	protected abstract void initProcessors() throws Exception;

}
