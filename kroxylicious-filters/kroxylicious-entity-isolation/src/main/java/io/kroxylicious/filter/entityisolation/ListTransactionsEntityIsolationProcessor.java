
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Entity isolation processor for FIND_COORDINATOR.
 * This implementation is handwritten as at v2, filtering the response requires information
 * from the request.
 */
class ListTransactionsEntityIsolationProcessor
        implements EntityIsolationProcessor<ListTransactionsRequestData, ListTransactionsResponseData, ListTransactionsEntityIsolationProcessor.RequestContext> {

    private static final Pattern ALL = Pattern.compile(".*");
    private static final Logger LOGGER = LoggerFactory.getLogger(ListTransactionsEntityIsolationProcessor.class);

    private final Set<EntityIsolation.ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    ListTransactionsEntityIsolationProcessor(Set<EntityIsolation.ResourceType> resourceTypes, EntityNameMapper mapper) {
        this.resourceTypes = Objects.requireNonNull(resourceTypes);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return (short) 0 <= apiVersion && apiVersion <= (short) 2;
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return (short) 0 <= apiVersion && apiVersion <= (short) 2;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                          short apiVersion,
                                                          ListTransactionsRequestData request,
                                                          FilterContext filterContext,
                                                          MapperContext mapperContext) {
        if (shouldMap(EntityIsolation.ResourceType.TRANSACTIONAL_ID)) {
            // Request Spec: all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
            // We don't want to rewrite the user's regular expression to accommodate
            // the isolation yet, instead we cache the RE into the context and apply that
            // to the response.
            var transactionalIdPattern = request.transactionalIdPattern();
            if (transactionalIdPattern != null && !transactionalIdPattern.isEmpty()) {
                try {
                    Pattern.compile(transactionalIdPattern);
                }
                catch (PatternSyntaxException pse) {
                    return filterContext.requestFilterResultBuilder().errorResponse(header, request, Errors.INVALID_REGULAR_EXPRESSION.exception()).completed();
                }
                // Idea: n.b. there's a half-way house where we pass the isolation prefix as
                // our own RE and apply the user's at the response stage, reducing the
                // need size of the response that the server needs to send to the proxy.
                request.setTransactionalIdPattern(null);
            }
        }

        return filterContext.forwardRequest(header, request);
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            @NonNull RequestContext requestContext,
                                                            ListTransactionsResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        log(filterContext, "response", ApiKeys.LIST_TRANSACTIONS, response);

        Pattern pat = Optional.of(requestContext).map(RequestContext::transactionalIdPattern).map(Pattern::compile).orElse(ALL);
        var transactionStatesIterator = response.transactionStates().iterator();
        while (transactionStatesIterator.hasNext()) {
            var transactionState = transactionStatesIterator.next();
            // process entity fields defined at this level
            if (shouldMap(EntityIsolation.ResourceType.TRANSACTIONAL_ID) && (short) 0 <= apiVersion && apiVersion <= (short) 2
                    && transactionState.transactionalId() != null) {
                if (inNamespace(mapperContext, EntityIsolation.ResourceType.TRANSACTIONAL_ID, transactionState.transactionalId())) {
                    var txnId = unmap(mapperContext, EntityIsolation.ResourceType.TRANSACTIONAL_ID, transactionState.transactionalId());
                    if (pat.matcher(txnId).find()) {
                        transactionState.setTransactionalId(txnId);
                        continue;
                    }
                }
                transactionStatesIterator.remove();
            }
        }
        log(filterContext, "response result", ApiKeys.LIST_TRANSACTIONS, response);
        return filterContext.forwardResponse(header, response);
    }

    private boolean shouldMap(EntityIsolation.ResourceType entityType) {
        return resourceTypes.contains(entityType);
    }

    private String map(MapperContext context, EntityIsolation.ResourceType resourceType, String originalName) {
        if (originalName == null || originalName.isEmpty()) {
            return originalName;
        }
        return mapper.map(context, resourceType, originalName);
    }

    private String unmap(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
        if (mappedName.isEmpty()) {
            return mappedName;
        }
        return mapper.unmap(context, resourceType, mappedName);
    }

    private boolean inNamespace(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
        return mapper.isInNamespace(context, resourceType, mappedName);
    }

    private static void log(FilterContext context, String description, ApiKeys key, ApiMessage message) {
        LOGGER.atDebug()
                .addArgument(() -> context.sessionId())
                .addArgument(() -> context.authenticatedSubject())
                .addArgument(description)
                .addArgument(key)
                .addArgument(message)
                .log("{} for {}: {} {}: {}");
    }

    @NonNull
    @Override
    public RequestContext createCorrelatedRequestContext(ListTransactionsRequestData request) {
        return new RequestContext(request.transactionalIdPattern());
    }

    record RequestContext(@Nullable String transactionalIdPattern) {}
}
