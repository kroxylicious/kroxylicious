/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

/**
 * Something that receives the result of an encryption or decryption operation
 */
public interface Receiver {
    /**
     * Receive the ciphertext (encryption) or the plaintext (decryption) associated with the given record..
     *
     * @param kafkaRecord The record on which to base the revised record
     * @param value The ciphertext or plaintext buffer.
     */
    void accept(Record kafkaRecord, ByteBuffer value, Header[] headers);
}
