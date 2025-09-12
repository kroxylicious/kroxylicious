/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.slf4j.Logger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.filter.encryption.config.UnresolvedKeyPolicy;
import io.kroxylicious.filter.encryption.decrypt.DecryptionManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionScheme;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformer;
import io.kroxylicious.kafka.transform.ApiVersionsResponseTransformers;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A filter for encrypting and decrypting records using envelope encryption
 * @param <K> The type of KEK reference
 */
public class RecordEncryptionFilter<K>
        implements ProduceRequestFilter, FetchResponseFilter, ApiVersionsResponseFilter {
    private static final Logger log = getLogger(RecordEncryptionFilter.class);
    private final TopicNameBasedKekSelector<K> kekSelector;

    // currently we must downgrade to a produce request version that does not support topic ids, we rely on
    // topic names in the messages when selecting keks.
    // todo remove once we have a facility to look up the topic name for a topic id
    private static final ApiVersionsResponseTransformer DOWNGRADE = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 12));

    private final EncryptionManager<K> encryptionManager;
    private final DecryptionManager decryptionManager;
    private final FilterThreadExecutor filterThreadExecutor;
    private final UnresolvedKeyPolicy unresolvedKeyPolicy;

    RecordEncryptionFilter(EncryptionManager<K> encryptionManager,
                           DecryptionManager decryptionManager,
                           TopicNameBasedKekSelector<K> kekSelector,
                           @NonNull FilterThreadExecutor filterThreadExecutor,
                           UnresolvedKeyPolicy unresolvedKeyPolicy) {
        this.kekSelector = kekSelector;
        this.encryptionManager = encryptionManager;
        this.decryptionManager = decryptionManager;
        this.filterThreadExecutor = filterThreadExecutor;
        this.unresolvedKeyPolicy = unresolvedKeyPolicy;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion,
                                                                 RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {
        return maybeEncodeProduce(request, context)
                .thenCompose(yy -> context.forwardRequest(header, request))
                .exceptionallyCompose(throwable -> {
                    final ApiException clientFacingException = getClientFacingException(throwable);
                    if (clientFacingException != null) {
                        return context.requestFilterResultBuilder()
                                .errorResponse(header, request, clientFacingException)
                                .completed();
                    }
                    else {
                        // returning a failed stage is effectively asking the runtime to kill the connection.
                        return CompletableFuture.failedStage(throwable);
                    }
                });
    }

    private static ApiException getClientFacingException(Throwable throwable) {
        if (isEncryptionException(throwable)) {
            return ((EncryptionException) throwable).getApiException();
        }
        else if (isEncryptionException(throwable.getCause())) {
            return ((EncryptionException) throwable.getCause()).getApiException();
        }
        else {
            return null;
        }
    }

    private CompletionStage<ProduceRequestData> maybeEncodeProduce(ProduceRequestData request, FilterContext context) {
        var plainRecordsTotal = RecordEncryptionMetrics.plainRecordsCounter(context.getVirtualClusterName());
        var encyptedRecordsTotal = RecordEncryptionMetrics.encryptedRecordsCounter(context.getVirtualClusterName());
        var topicNameToData = request.topicData().stream().collect(Collectors.toMap(TopicProduceData::name, Function.identity()));
        CompletionStage<TopicNameKekSelection<K>> keks = filterThreadExecutor.completingOnFilterThread(kekSelector.selectKek(topicNameToData.keySet()));
        return keks // figure out what keks we need
                .thenCompose(kekSelection -> {
                    Set<String> unresolvedTopicNames = kekSelection.unresolvedTopicNames();
                    if (!unresolvedTopicNames.isEmpty() && unresolvedKeyPolicy == UnresolvedKeyPolicy.REJECT) {
                        return CompletableFuture.failedFuture(new UnresolvedKeyException("failed to resolve key for: " + unresolvedTopicNames));
                    }

                    generatePlainRecordsMetrics(plainRecordsTotal, unresolvedTopicNames, topicNameToData);

                    var futures = kekSelection.topicNameToKekId().entrySet().stream().flatMap(e -> {
                        String topicName = e.getKey();
                        var kekId = e.getValue();
                        TopicProduceData tpd = topicNameToData.get(topicName);
                        return tpd.partitionData().stream().map(ppd -> {
                            MemoryRecords records = (MemoryRecords) ppd.records();
                            return encryptionManager.encrypt(
                                    topicName,
                                    ppd.index(),
                                    new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                                    records,
                                    context::createByteBufferOutputStream)
                                    .thenApply(ppd::setRecords)
                                    .thenApply(produceData -> {
                                        encyptedRecordsTotal
                                                .withTags(RecordEncryptionMetrics.TOPIC_NAME, topicName)
                                                .increment(RecordEncryptionUtil.totalRecordsInBatches((MemoryRecords) produceData.records()));
                                        return null;
                                    });
                        });
                    }).toList();
                    return RecordEncryptionUtil.join(futures).thenApply(x -> request);
                }).exceptionallyCompose(throwable -> {
                    log.atWarn().setMessage("failed to encrypt records, cause message: {}")
                            .addArgument(throwable.getMessage())
                            .setCause(log.isDebugEnabled() ? throwable : null)
                            .log();
                    return CompletableFuture.failedStage(throwable);
                });
    }

    private void generatePlainRecordsMetrics(Meter.MeterProvider<Counter> plainRecordsTotal, Set<String> unresolvedTopicNames,
                                             Map<String, TopicProduceData> topicNameToData) {
        topicNameToData.entrySet().stream()
                .filter(topicDataEntry -> unresolvedTopicNames.contains(topicDataEntry.getKey()))
                .forEach(topicData -> topicData.getValue().partitionData()
                        .forEach(produceData -> plainRecordsTotal
                                .withTags(RecordEncryptionMetrics.TOPIC_NAME, topicData.getKey())
                                .increment(RecordEncryptionUtil.totalRecordsInBatches((MemoryRecords) produceData.records()))));
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        return maybeDecodeFetch(response.responses(), context)
                .thenCompose(responses -> context.forwardResponse(header, response.setResponses(responses)))
                .exceptionallyCompose(throwable -> {
                    if (throwable.getCause() instanceof UnknownKeyException) {
                        // #maybeDecodePartitions will have set the RESOURCE_NOT_FOUND error code on the partition(s) that failed to decrypt
                        // and will have logged the affected topic-partitions.
                        // Remove all the records from the whole fetch to avoid the possibility that the client processes an incomplete response.
                        response.responses().forEach(r -> r.partitions().forEach(p -> p.setRecords(MemoryRecords.EMPTY)));
                        return context.forwardResponse(header, response);
                    }
                    else {
                        log.atWarn().setMessage("Failed to process records, connection will be closed, cause message: {}")
                                .addArgument(throwable.getMessage())
                                .setCause(log.isDebugEnabled() ? throwable : null)
                                .log();
                        // returning a failed stage is effectively asking the runtime to kill the connection.
                        return CompletableFuture.failedStage(throwable);
                    }
                });
    }

    private CompletionStage<List<FetchableTopicResponse>> maybeDecodeFetch(List<FetchableTopicResponse> topics, FilterContext context) {
        List<CompletionStage<FetchableTopicResponse>> result = new ArrayList<>(topics.size());
        for (FetchableTopicResponse topicData : topics) {
            result.add(maybeDecodePartitions(topicData.topic(), topicData.partitions(), context).thenApply(kk -> {
                topicData.setPartitions(kk);
                return topicData;
            }));
        }
        return RecordEncryptionUtil.join(result);
    }

    private CompletionStage<List<PartitionData>> maybeDecodePartitions(String topicName,
                                                                       List<PartitionData> partitions,
                                                                       FilterContext context) {
        List<CompletionStage<PartitionData>> result = new ArrayList<>(partitions.size());
        for (PartitionData partitionData : partitions) {
            if (!(partitionData.records() instanceof MemoryRecords)) {
                throw new IllegalStateException();
            }
            var stage = maybeDecodeRecords(topicName, partitionData, (MemoryRecords) partitionData.records(), context)
                    .exceptionallyCompose(t -> {
                        var cause = t.getCause();
                        if (cause instanceof UnknownKeyException) {
                            log.atWarn()
                                    .setMessage("Failed to decrypt record in topic-partition {}-{} owing to key not found condition. "
                                            + "This will be reported to the client as a RESOURCE_NOT_FOUND(91). Client may see a message like 'Unexpected error code 91 while fetching at offset' (java) or "
                                            + "'Request illegally referred to resource that does not exist' (librdkafka). "
                                            + "Cause message: {}. "
                                            + "Raise log level to DEBUG to see the stack.")
                                    .addArgument(topicName)
                                    .addArgument(partitionData.partitionIndex())
                                    .addArgument(cause.getMessage())
                                    .setCause(log.isDebugEnabled() ? cause : null)
                                    .log();
                            partitionData.setErrorCode(Errors.RESOURCE_NOT_FOUND.code());
                        }
                        return CompletableFuture.failedFuture(t);
                    });
            result.add(stage);
        }
        return RecordEncryptionUtil.join(result);
    }

    private CompletionStage<PartitionData> maybeDecodeRecords(String topicName,
                                                              PartitionData fpr,
                                                              MemoryRecords memoryRecords,
                                                              FilterContext context) {
        return decryptionManager.decrypt(
                topicName,
                fpr.partitionIndex(),
                memoryRecords,
                context::createByteBufferOutputStream)
                .thenApply(fpr::setRecords);
    }

    private static boolean isEncryptionException(Throwable throwable) {
        return throwable instanceof EncryptionException;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        return context.forwardResponse(header, DOWNGRADE.transform(response));
    }
}
