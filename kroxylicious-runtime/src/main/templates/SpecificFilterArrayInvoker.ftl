<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ${outputPackage};

import java.util.concurrent.CompletionStage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Invoker for Filters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
class SpecificFilterArrayInvoker implements FilterInvoker {

    private static final FilterInvoker[] HANDLE_NOTHING = createHandleNothing();

    private final FilterInvoker[] requestInvokers;
    private final FilterInvoker[] responseInvokers;

    SpecificFilterArrayInvoker(Filter filter) {
        Map<Integer, FilterInvoker> requestInvokers = new HashMap<>();
        Map<Integer, FilterInvoker> responseInvokers = new HashMap<>();
        <#list messageSpecs as messageSpec>
        if (filter instanceof ${messageSpec.name}Filter) {
            ${messageSpec.type?lower_case}Invokers.put(${messageSpec.apiKey.get()}, new ${messageSpec.name}FilterInvoker((${messageSpec.name}Filter) filter));
        }
        </#list>
        this.requestInvokers = createFrom(requestInvokers);
        this.responseInvokers = createFrom(responseInvokers);
    }

    /**
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     * @param apiKey The request api key.
     * @param apiVersion The request api version.
     * @param header The request header.
     * @param body The request body.
     * @param filterContext The filter context.
     */
    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage body,
                                                          FilterContext filterContext) {
        // We wrap the array lookup in a switch based on the API Key as it supports JIT optimisations around method dispatch.
        // See the InvokerDispatchBenchmark micro benchmark for a comparison
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} ->
                requestInvokers[apiKey.id].onRequest(apiKey, apiVersion, header, body, filterContext);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };

    }

    /**
     * Apply the filter to the given {@code header} and {@code body} using the given {@code filterContext}.
     * @param apiKey The request api key.
     * @param apiVersion The api version.
     * @param header The request header.
     * @param body The request body.
     * @param filterContext The filter context.
     */
    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage body,
                                                            FilterContext filterContext) {
        // We wrap the array lookup in a switch based on the API Key as it supports JIT optimisations around method dispatch.
        // See the InvokerDispatchBenchmark micro benchmark for a comparison
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
            case ${retrieveApiKey(messageSpec)} ->
                    responseInvokers[apiKey.id].onResponse(apiKey, apiVersion, header, body, filterContext);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };

    }

    /**
     * <p>Determines whether a request with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per request,
     * or that two consecutive calls refer to the same request.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldHandleRequest} on request A</li>
     *     <li>{@code shouldHandleRequest} on request B</li>
     *     <li>{@code shouldHandleRequest} on request A</li>
     *     <li>{@code onRequest} on request A</li>
     *     <li>{@code onRequest} on request B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return true if request should be deserialized
     */
    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        // We wrap the array lookup in a switch based on the API Key as it supports JIT optimisations around method dispatch.
        // See the InvokerDispatchBenchmark micro benchmark for a comparison
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} ->
                    requestInvokers[apiKey.id].shouldHandleRequest(apiKey, apiVersion);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };

    }

    /**
     * <p>Determines whether a response with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per response,
     * or that two consecutive calls refer to the same response.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldHandleResponse} on response A</li>
     *     <li>{@code shouldHandleResponse} on response B</li>
     *     <li>{@code shouldHandleResponse} on response A</li>
     *     <li>{@code apply} on response A</li>
     *     <li>{@code apply} on response B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return true if response should be deserialized
     */
    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            // We wrap the array lookup in a switch based on the API Key as it supports JIT optimisations around method dispatch.
            // See the InvokerDispatchBenchmark micro benchmark for a comparison
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
            case ${retrieveApiKey(messageSpec)} ->
                    responseInvokers[apiKey.id].shouldHandleResponse(apiKey, apiVersion);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
        };

    }

    /**
    * Check if a Filter implements any of the Specific Request Filter interfaces
    * @param filter the filter
    * @return true if the filter implements any Specific Request Filter interfaces
    */
    public static boolean implementsAnySpecificRequestFilterInterface(Filter filter) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
        if (filter instanceof ${messageSpec.name}Filter) {
            return true;
        }
    </#if>
</#list>
        return false;
    }

    /**
    * Check if a Filter implements any of the Specific Response Filter interfaces
    * @param filter the filter
    * @return true if the filter implements any Specific Response Filter interfaces
    */
    public static boolean implementsAnySpecificResponseFilterInterface(Filter filter) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
        if (filter instanceof ${messageSpec.name}Filter) {
            return true;
        }
    </#if>
</#list>
        return false;
    }

    private static FilterInvoker[] createHandleNothing() {
        FilterInvoker[] filterInvokers = emptyInvokerArraySizedForMessageTypes();
        Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).forEach(value -> filterInvokers[value] = FilterInvokers.handleNothingInvoker());
        return filterInvokers;
    }

    private static FilterInvoker[] createFrom(Map<Integer, FilterInvoker> filterInvokersByApiMessageId) {
        if (filterInvokersByApiMessageId.isEmpty()) {
            return HANDLE_NOTHING;
        }
        FilterInvoker[] filterInvokers = emptyInvokerArraySizedForMessageTypes();
        Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).forEach(value -> filterInvokers[value] = filterInvokersByApiMessageId.getOrDefault(value, FilterInvokers.handleNothingInvoker()));
        return filterInvokers;
    }

    private static FilterInvoker[] emptyInvokerArraySizedForMessageTypes() {
        OptionalInt maybeMaxId = Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).max();
        if (maybeMaxId.isEmpty()) {
            throw new IllegalStateException("no maximum id found");
        }
        int arraySize = maybeMaxId.getAsInt() + 1;
        return new FilterInvoker[arraySize];
    }

}
