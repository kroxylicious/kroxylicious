/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.Record;

public interface RecordEncryptionScheme<K> {

    CompletionStage<Record> encrypt(Record plainTextRecord, RequestHeaderData recordHeaders, ProduceRequestData produceRequestData);

    CompletionStage<Record> decrypt(Record encipheredRecord, ResponseHeaderData responseHeaderData, FetchResponseData fetchResponseData);
}
