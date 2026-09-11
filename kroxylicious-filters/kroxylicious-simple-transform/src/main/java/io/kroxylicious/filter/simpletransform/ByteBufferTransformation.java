/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.simpletransform;

import java.nio.ByteBuffer;

/**
 * A transformation of the key or value of a produce record.
 */
@FunctionalInterface
public interface ByteBufferTransformation {

    /**
     * Transforms the given buffer.
     * @param topicName The name of the topic to which the record being transformed belongs.
     * @param original The buffer to transform.
     * @return The transformed buffer.
     */
    ByteBuffer transform(String topicName, ByteBuffer original);
}