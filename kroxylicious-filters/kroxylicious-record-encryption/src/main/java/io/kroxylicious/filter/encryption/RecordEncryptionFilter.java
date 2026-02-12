/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.ShareFetchableTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
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
import io.kroxylicious.proxy.filter.ShareFetchResponseFilter;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A filter for encrypting and decrypting records using envelope encryption
 * @param <K> The type of KEK reference
 */
public class RecordEncryptionFilter<K>
        implements ProduceRequestFilter, FetchResponseFilter, ShareFetchResponseFilter, ApiVersionsResponseFilter {
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
        var encryptedRecordsTotal = RecordEncryptionMetrics.encryptedRecordsCounter(context.getVirtualClusterName());
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
                                        encryptedRecordsTotal
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
    public CompletionStage<ResponseFilterResult> onShareFetchResponse(short apiVersion, ResponseHeaderData header, ShareFetchResponseData response,
                                                                      FilterContext context) {
        Set<Uuid> topicIds = response.responses().stream().map(ShareFetchableTopicResponse::topicId).collect(Collectors.toSet());
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> maybeDecodeShareFetch(topicNameMapping, response.responses(), context)
                .thenCompose(topicResponses -> {
                    ShareFetchResponseData.ShareFetchableTopicResponseCollection collection = new ShareFetchResponseData.ShareFetchableTopicResponseCollection();
                    // danger, the share fetch response uses a collection where the `add` method can silently do nothing
                    // if the element being added already has element link fields populated it will not be added.
                    // we reset next and prev so the existing elements can be added without calling `duplicate()`
                    // which would copy the memory records again.
                    // this may leave the original collection in an inconsistent state, but we are finished using it
                    topicResponses.forEach(topicResponse -> {
                        topicResponse.setNext(ImplicitLinkedHashCollection.INVALID_INDEX);
                        topicResponse.setPrev(ImplicitLinkedHashCollection.INVALID_INDEX);
                        collection.mustAdd(topicResponse);
                    });
                    return context.forwardResponse(header, response.setResponses(collection));
                })
                .exceptionallyCompose(throwable -> {
                    if (throwable.getCause() instanceof UnknownKeyException) {
                        // #maybeDecodePartitions will have set the RESOURCE_NOT_FOUND error code on the partition(s) that failed to decrypt
                        // and will have logged the affected topic-partitions.
                        // Remove all the records from the whole fetch to avoid the possibility that the client processes an incomplete response.
                        response.responses().forEach(topicResponse -> topicResponse.partitions().forEach(p -> p.setRecords(MemoryRecords.EMPTY)));
                        return context.forwardResponse(header, response);
                    }
                    else {
                        // returning a failed stage is effectively asking the runtime to kill the connection.
                        return logAndCreateFailedStage(throwable);
                    }
                }));
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
                        // returning a failed stage is effectively asking the runtime to kill the connection.
                        return logAndCreateFailedStage(throwable);
                    }
                });
    }

    private static CompletionStage<ResponseFilterResult> logAndCreateFailedStage(Throwable throwable) {
        log.atWarn().setMessage("Failed to process records, connection will be closed, cause message: {}. Raise log level to DEBUG to see the stack.")
                .addArgument(throwable.getMessage())
                .setCause(log.isDebugEnabled() ? throwable : null)
                .log();
        return CompletableFuture.failedStage(throwable);
    }

    private CompletionStage<List<ShareFetchableTopicResponse>> maybeDecodeShareFetch(TopicNameMapping mapping, Collection<ShareFetchableTopicResponse> topics,
                                                                                     FilterContext context) {
        List<CompletionStage<ShareFetchableTopicResponse>> result = new ArrayList<>(topics.size());
        for (ShareFetchableTopicResponse topicData : topics) {
            TopicNameMappingException failure = mapping.failures().get(topicData.topicId());
            String topicName = mapping.topicNames().get(topicData.topicId());
            List<ShareFetchResponseData.PartitionData> partitions = topicData.partitions();
            if (topicName != null) {
                result.add(maybeDecodePartitions(topicName, partitions, context, ShareFetchResponseData.PartitionData::records,
                        ShareFetchResponseData.PartitionData::partitionIndex,
                        partitionData -> partitionData::setRecords, ShareFetchResponseData.PartitionData::setErrorCode).thenApply(kk -> {
                            topicData.setPartitions(kk);
                            return topicData;
                        }));
            }
            else {
                result.add(setAllPartitionsToError(topicData, failure));
            }
        }
        return RecordEncryptionUtil.join(result);
    }

    private static CompletableFuture<ShareFetchableTopicResponse> setAllPartitionsToError(ShareFetchableTopicResponse topicData,
                                                                                          TopicNameMappingException failure) {
        for (ShareFetchResponseData.PartitionData partition : topicData.partitions()) {
            partition.setRecords(MemoryRecords.EMPTY);
            Errors error = failure != null ? failure.getError() : Errors.UNKNOWN_SERVER_ERROR;
            partition.setErrorCode(error.code());
            partition.setErrorMessage(error.message());
        }
        return CompletableFuture.completedFuture(topicData);
    }

    private CompletionStage<List<FetchableTopicResponse>> maybeDecodeFetch(List<FetchableTopicResponse> topics, FilterContext context) {
        List<CompletionStage<FetchableTopicResponse>> result = new ArrayList<>(topics.size());
        for (FetchableTopicResponse topicData : topics) {
            String topicName = topicData.topic();
            List<PartitionData> partitions = topicData.partitions();
            result.add(maybeDecodePartitions(topicName, partitions, context, PartitionData::records, PartitionData::partitionIndex,
                    partitionData -> partitionData::setRecords,
                    PartitionData::setErrorCode).thenApply(kk -> {
                        topicData.setPartitions(kk);
                        return topicData;
                    }));
        }
        return RecordEncryptionUtil.join(result);
    }

    private <T> CompletionStage<List<T>> maybeDecodePartitions(String topicName,
                                                               List<T> partitions,
                                                               FilterContext context,
                                                               Function<T, BaseRecords> recordExtractor,
                                                               ToIntFunction<T> partitionIndexExtractor,
                                                               Function<T, Function<MemoryRecords, T>> setRecords,
                                                               BiFunction<T, Short, T> errorsConsumer) {
        List<CompletionStage<T>> result = new ArrayList<>(partitions.size());
        for (T partitionData : partitions) {
            BaseRecords records = recordExtractor.apply(partitionData);
            if (!(records instanceof MemoryRecords)) {
                throw new IllegalStateException();
            }
            var stage = maybeDecodeRecords(topicName, (MemoryRecords) records, context, partitionIndexExtractor.applyAsInt(partitionData),
                    setRecords.apply(partitionData))
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
                                    .addArgument(() -> partitionIndexExtractor.applyAsInt(partitionData))
                                    .addArgument(cause.getMessage())
                                    .setCause(log.isDebugEnabled() ? cause : null)
                                    .log();
                            errorsConsumer.apply(partitionData, Errors.RESOURCE_NOT_FOUND.code());
                        }
                        return CompletableFuture.failedFuture(t);
                    });
            result.add(stage);
        }
        return RecordEncryptionUtil.join(result);
    }

    private <T> CompletionStage<T> maybeDecodeRecords(String topicName,
                                                      MemoryRecords memoryRecords,
                                                      FilterContext context, int partition,
                                                      Function<MemoryRecords, T> setRecords) {
        return decryptionManager.decrypt(
                topicName,
                partition,
                memoryRecords,
                context::createByteBufferOutputStream)
                .thenApply(setRecords);
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
