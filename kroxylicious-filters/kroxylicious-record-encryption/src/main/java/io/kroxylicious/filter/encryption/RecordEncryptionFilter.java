/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.MemoryRecords;
import org.slf4j.Logger;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.filter.encryption.decrypt.DecryptionManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionScheme;
import io.kroxylicious.filter.encryption.policy.TopicEncryptionPolicyResolver;
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
        implements ProduceRequestFilter, FetchResponseFilter {
    private static final Logger log = getLogger(RecordEncryptionFilter.class);
    private final TopicNameBasedKekSelector<K> kekSelector;

    private final EncryptionManager<K> encryptionManager;
    private final DecryptionManager decryptionManager;
    private final FilterThreadExecutor filterThreadExecutor;
    private final TopicEncryptionPolicyResolver policyResolver;

    RecordEncryptionFilter(EncryptionManager<K> encryptionManager,
                           DecryptionManager decryptionManager,
                           TopicNameBasedKekSelector<K> kekSelector,
                           @NonNull FilterThreadExecutor filterThreadExecutor,
                           TopicEncryptionPolicyResolver policyResolver) {
        this.kekSelector = kekSelector;
        this.encryptionManager = encryptionManager;
        this.decryptionManager = decryptionManager;
        this.filterThreadExecutor = filterThreadExecutor;
        this.policyResolver = policyResolver;
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
        var topicNameToData = request.topicData().stream().collect(Collectors.toMap(TopicProduceData::name, Function.identity()));
        Set<String> topicNames = topicNameToData.keySet();
        return policyResolver.apply(topicNames).thenCompose(
                topicNameToPolicy -> {
                    Set<String> topicsToResolve = topicNames.stream().filter(t -> topicNameToPolicy.get(t).shouldAttemptKeyResolution()).collect(Collectors.toSet());
                    CompletionStage<TopicNameKekSelection<K>> keks = filterThreadExecutor.completingOnFilterThread(kekSelector.selectKek(topicsToResolve));
                    return keks // figure out what keks we need
                            .thenCompose(kekSelection -> {
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
                                                .thenApply(ppd::setRecords);
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
                });
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        return maybeDecodeFetch(response.responses(), context)
                .thenCompose(responses -> context.forwardResponse(header, response.setResponses(responses)))
                .exceptionallyCompose(throwable -> {
                    log.atWarn().setMessage("failed to decrypt records, cause message: {}")
                            .addArgument(throwable.getMessage())
                            .setCause(log.isDebugEnabled() ? throwable : null)
                            .log();
                    return CompletableFuture.failedStage(throwable);
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
            result.add(maybeDecodeRecords(topicName, partitionData, (MemoryRecords) partitionData.records(), context));
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
}
