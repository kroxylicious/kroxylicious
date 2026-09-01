/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.MutableRecordBatch;
import io.kroxylicious.kafka.common.record.internal.Record;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.kafka.common.utils.BufferSupplier;
import io.kroxylicious.kafka.common.utils.CloseableIterator;

/**
 * Utility methods shared between the encryption and decryption paths.
 */
public class RecordEncryptionUtil {

    private RecordEncryptionUtil() {
    }

    /**
     * Joins the given completion stages into a single completion stage which completes with the list of their results.
     * @param stages the completion stages to join.
     * @param <T> the result type of the completion stages.
     * @return a completion stage that completes with the list of the results of the given stages.
     */
    @SuppressWarnings("unchecked")
    public static <T> CompletionStage<List<T>> join(List<? extends CompletionStage<T>> stages) {
        CompletableFuture<T>[] futures = stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> Stream.of(futures).map(CompletableFuture::join).toList());
    }

    /**
     * Counts the records in all the batches of the given memory records.
     * @param records the memory records.
     * @return the total number of records.
     */
    public static int totalRecordsInBatches(@NonNull MemoryRecords records) {
        int totalRecords = 0;
        for (MutableRecordBatch batch : records.batches()) {
            totalRecords += recordCount(batch);
        }
        return totalRecords;
    }

    private static int recordCount(@NonNull MutableRecordBatch batch) {
        Integer count = batch.countOrNull();
        if (count == null) {
            // for magic <2 count will be null
            try (CloseableIterator<Record> iterator = batch.skipKeyValueIterator(BufferSupplier.NO_CACHING)) {
                int c = 0;
                while (iterator.hasNext()) {
                    c++;
                    iterator.next();
                }
                count = c;
            }
        }
        return count;
    }
}
