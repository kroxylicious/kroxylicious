/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.slf4j.Logger;

import io.kroxylicious.filter.encryption.dek.DekException;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A filter for encrypting and decrypting records using envelope encryption
 * @param <K> The type of KEK reference
 */
public class EnvelopeEncryptionFilter<K>
        implements ProduceRequestFilter, ProduceResponseFilter, FetchResponseFilter {
    private static final Logger log = getLogger(EnvelopeEncryptionFilter.class);
    private final TopicNameBasedKekSelector<K> kekSelector;

    private final EncryptionManager<K> encryptionManager;
    private final DecryptionManager decryptionManager;
    private final FilterThreadExecutor filterThreadExecutor;

    /**
     * Errors omitted from inflight upstream ProduceRequests to be married up with their ProduceResponses
     * once they're received. Key for outer map is the correlation id, Key for the inner maps is the topic name.
     */
    private final Map<Integer, Map<String, List<ProduceResponseData.PartitionProduceResponse>>> produceErrors = new HashMap<>();

    EnvelopeEncryptionFilter(EncryptionManager<K> encryptionManager,
                             DecryptionManager decryptionManager,
                             TopicNameBasedKekSelector<K> kekSelector,
                             @NonNull FilterThreadExecutor filterThreadExecutor) {
        this.kekSelector = kekSelector;
        this.encryptionManager = encryptionManager;
        this.decryptionManager = decryptionManager;
        this.filterThreadExecutor = filterThreadExecutor;
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletionStage<List<T>> join(List<? extends CompletionStage<T>> stages) {
        CompletableFuture<T>[] futures = stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(ignored -> Stream.of(futures).map(CompletableFuture::join).toList());
    }

    private void appendPartitionErrors(ProduceResponseData.TopicProduceResponseCollection responses,
                                       Map<String, List<ProduceResponseData.PartitionProduceResponse>> topicToPartitionErrors) {
        topicToPartitionErrors.forEach((topicName, prds) -> {
            var topicProduceResponse = responses.find(topicName);
            if (topicProduceResponse == null) {
                topicProduceResponse = new ProduceResponseData.TopicProduceResponse();
                topicProduceResponse.setName(topicName);
                topicProduceResponse.setPartitionResponses(prds);
                responses.add(topicProduceResponse);
            }
            else {
                topicProduceResponse.partitionResponses().addAll(prds);
            }
        });
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion,
                                                                 RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {
        var topicNameToData = request.topicData().stream().collect(Collectors.toMap(TopicProduceData::name, Function.identity()));
        Map<String, CompletionStage<Optional<K>>> keks = kekSelector.selectKek(topicNameToData.keySet());

        CompletableFuture<RequestFilterResult> cf = new CompletableFuture<>();

        filterThreadExecutor.completingOnFilterThread(EnvelopeEncryptionFilter.join(new ArrayList<>(keks.values()))) // figure out what keks we need
                .whenComplete((ignoredValue, ignoredException) -> {
                    var partitioned = partitionBySuccess(keks);
                    var withoutErrors = succeeded(partitioned.getOrDefault(true, List.of()));
                    var withErrors = failed(partitioned.getOrDefault(false, List.of()));
                    CompletionStage<RequestFilterResult> requestFilterResultCompletionStage;
                    if (withoutErrors.isEmpty()) {
                        // Respond directly, avoiding sending an empty produce request to the broker
                        requestFilterResultCompletionStage = sendShortcircuitResponse(header, context, withErrors, topicNameToData);
                    }
                    else {
                        requestFilterResultCompletionStage = encryptAndForward(header, request, context, withoutErrors, withErrors, topicNameToData);
                    }
                    requestFilterResultCompletionStage.whenComplete((value, exception) -> {
                        if (exception != null) {
                            cf.completeExceptionally(exception);
                        }
                        else {
                            cf.complete(value);
                        }
                    });
                });
        return cf;
    }

    private <K, V> List<Map.Entry<K, V>> succeeded(List<Map.Entry<K, CompletionStage<V>>> list) {
        return list.stream().map(entry -> {
            CompletableFuture<V> cf = entry.getValue().toCompletableFuture();
            if (!cf.isDone()) {
                throw new IllegalStateException();
            }
            return Map.entry(entry.getKey(), cf.join());
        }).toList();
    }

    private <K, V> List<Map.Entry<K, Throwable>> failed(List<Map.Entry<K, CompletionStage<V>>> list) {
        return list.stream().map(entry -> Map.entry(entry.getKey(), causeOfFailedStage(entry.getValue()))).toList();
    }

    @NonNull
    private CompletionStage<RequestFilterResult> sendShortcircuitResponse(RequestHeaderData header, FilterContext context,
                                                                          List<Map.Entry<String, Throwable>> withErrors,
                                                                          Map<String, TopicProduceData> topicNameToData) {
        try {
            ProduceResponseData.TopicProduceResponseCollection topicProduceResponses = new ProduceResponseData.TopicProduceResponseCollection();
            appendPartitionErrors(topicProduceResponses, toPartitionErrs(withErrors, topicNameToData));
            return CompletableFuture.completedStage(context.requestFilterResultBuilder()
                    .shortCircuitResponse(new ResponseHeaderData().setCorrelationId(header.correlationId()),
                            new ProduceResponseData()
                                    // .setThrottleTimeMs()
                                    .setResponses(topicProduceResponses))
                    .build());
        }
        catch (Exception e) {
            return CompletableFuture.failedStage(e);
        }
    }

    private CompletionStage<RequestFilterResult> encryptAndForward(RequestHeaderData header, ProduceRequestData request, FilterContext context,
                                                                   List<Map.Entry<String, Optional<K>>> withoutErrors,
                                                                   List<Map.Entry<String, Throwable>> withErrors,
                                                                   Map<String, TopicProduceData> topicNameToData) {
        final CompletionStage<RequestFilterResult> requestFilterResultCompletionStage;
        var futures = encryptWithKeks(context, withoutErrors, topicNameToData);
        requestFilterResultCompletionStage = join(futures).thenApply(x -> request)
                .exceptionallyCompose(throwable -> {
                    log.atWarn().setMessage("failed to encrypt records, cause message: {}")
                            .addArgument(throwable.getMessage())
                            .setCause(log.isDebugEnabled() ? throwable : null)
                            .log();
                    return CompletableFuture.failedStage(throwable);
                })
                .thenCompose(yy -> {
                    removeTopicsWithErrors(header, request, withErrors, topicNameToData);
                    return context.forwardRequest(header, request);
                });
        return requestFilterResultCompletionStage;
    }

    private void removeTopicsWithErrors(RequestHeaderData header,
                                        ProduceRequestData request,
                                        List<Map.Entry<String, Throwable>> withErrors,
                                        Map<String, TopicProduceData> topicNameToData) {
        if (!withErrors.isEmpty()) {
            var topicData = request.topicData();
            for (var err : withErrors) {
                topicData.remove(topicData.find(err.getKey()));
            }
            produceErrors.put(header.correlationId(), toPartitionErrs(withErrors, topicNameToData));
        }
    }

    private <T> @NonNull Map<Boolean, List<Map.Entry<String, CompletionStage<T>>>> partitionBySuccess(Map<String, CompletionStage<T>> map) {
        return map.entrySet().stream().collect(Collectors.partitioningBy(entry -> {
            try {
                entry.getValue().toCompletableFuture().join();
                return true;
            }
            catch (CompletionException | CancellationException e) {
                return false;
            }
        }));
    }

    private static @NonNull Map<String, List<ProduceResponseData.PartitionProduceResponse>> toPartitionErrs(
                                                                                                            List<Map.Entry<String, Throwable>> withErrors,
                                                                                                            Map<String, TopicProduceData> topicNameToData) {
        return withErrors.stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    String topicName = entry.getKey();
                    Throwable cause = entry.getValue();
                    return topicNameToData.get(topicName).partitionData().stream()
                            .map(ppd -> {
                                var partitionProduceResponse = new ProduceResponseData.PartitionProduceResponse();
                                handleExceptionDuringProduce(cause,
                                        (error, msg) -> {
                                            if (cause instanceof UnknownAliasException) {
                                                if (log.isInfoEnabled()) {
                                                    log.info(
                                                            "UnknownAliasException from KMS for alias '{}' while handling Produce request for topic '{}'; returning error code {}({}) and message '{}' to client.",
                                                            cause.getMessage(), topicName, error, error.code(), msg);
                                                }
                                            }
                                            else {
                                                if (log.isInfoEnabled()) {
                                                    log.info(
                                                            "Exception while handling Produce request for topic '{}'; returning error code {}({}) and message '{}' to client.",
                                                            topicName, error, error.code(), msg, cause);
                                                }
                                            }
                                            partitionProduceResponse
                                                    .setIndex(ppd.index())
                                                    .setErrorCode(error.code())
                                                    .setErrorMessage(msg);
                                        });
                                return partitionProduceResponse;
                            })
                            .toList();
                }));
    }

    @NonNull
    private List<CompletionStage<ProduceRequestData.PartitionProduceData>> encryptWithKeks(FilterContext context,
                                                                                           List<Map.Entry<String, Optional<K>>> withoutErrors,
                                                                                           Map<String, TopicProduceData> topicNameToData) {
        return withoutErrors.stream().flatMap(e -> {
            String topicName = e.getKey();
            var kekId = e.getValue().orElse(null);
            TopicProduceData tpd = topicNameToData.get(topicName);
            return tpd.partitionData().stream().map(ppd -> {
                // handle case where this topic is to be left unencrypted
                if (kekId == null) {
                    return CompletableFuture.completedStage(ppd);
                }
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
    }

    private static void handleExceptionDuringProduce(@NonNull Throwable cause,
                                                     @NonNull BiConsumer<Errors, String> consumer) {

        final Errors error;
        final String clientMessage;
        try {
            throw cause;
        }
        catch (UnknownKeyException | UnknownAliasException e) {
            error = Errors.RESOURCE_NOT_FOUND;
            clientMessage = "Encryption key or alias not known to KMS. See proxy logs for details.";
        }
        catch (KmsException | EncryptionException | DekException e) {
            error = Errors.UNKNOWN_SERVER_ERROR;
            clientMessage = "Unexpected error. See proxy logs for details.";
            log.warn("", e);
        }
        catch (Throwable t) {
            error = Errors.UNKNOWN_SERVER_ERROR;
            clientMessage = "Unexpected error. See proxy logs for details.";
            log.warn("", t);
        }
        consumer.accept(error, clientMessage);
    }

    private static @NonNull Throwable causeOfFailedStage(@NonNull CompletionStage<?> failedStage) {
        Throwable cause;
        try {
            var cf = failedStage.toCompletableFuture();
            if (!cf.isDone()) {
                return new IllegalStateException("Expected a done stage");
            }
            cf.join();
            return new IllegalStateException("Expected a failed stage");
        }
        catch (CompletionException e) {
            cause = e.getCause();
        }
        catch (CancellationException e) {
            cause = e;
        }
        if (cause == null) {
            return new IllegalStateException("Expected a cause");
        }
        return cause;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion,
                                                                   ResponseHeaderData header,
                                                                   ProduceResponseData response,
                                                                   FilterContext context) {
        var errMap = produceErrors.remove(header.correlationId());
        if (errMap != null) {
            ProduceResponseData.TopicProduceResponseCollection responses = response.responses();
            appendPartitionErrors(responses, errMap);
        }
        return context.forwardResponse(header, response);
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
        return join(result);
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
        return join(result);
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

}
