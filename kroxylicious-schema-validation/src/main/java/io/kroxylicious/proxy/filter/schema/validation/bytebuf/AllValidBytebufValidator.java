/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;

class AllValidBytebufValidator implements BytebufValidator {

    @Override
    public Result validate(ByteBuffer buffer, int length, Record record, boolean isKey) {
        return Result.VALID;
    }
}
