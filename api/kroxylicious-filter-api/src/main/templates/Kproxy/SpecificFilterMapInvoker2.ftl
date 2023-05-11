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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Invoker for KrpcFilters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
class SpecificFilterMapInvoker2 implements FilterInvoker {

    private final Map<ApiKeys, FilterInvoker> requestInvokers;
    private final Map<ApiKeys, FilterInvoker> responseInvokers;

    SpecificFilterMapInvoker2(KrpcFilter filter) {
        this.requestInvokers = new HashMap<>();
        this.responseInvokers = new HashMap<>();
        <#list messageSpecs as messageSpec>
        this.${messageSpec.type?lower_case}Invokers.put(ApiKeys.${retrieveApiKey(messageSpec)}, filter instanceof ${messageSpec.name}Filter ? new ${messageSpec.name}FilterInvoker((${messageSpec.name}Filter) filter) :
                FilterInvokers.handleNothingInvoker());
        </#list>
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
    public void onRequest(ApiKeys apiKey,
                           short apiVersion,
                           RequestHeaderData header,
                           ApiMessage body,
                           KrpcFilterContext filterContext) {
        FilterInvoker invoker = requestInvokers.computeIfAbsent(apiKey, (apiKeys -> FilterInvokers.handleNothingInvoker()));
        invoker.onRequest(apiKey, apiVersion, header, body, filterContext);
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
    public void onResponse(ApiKeys apiKey,
                            short apiVersion,
                            ResponseHeaderData header,
                            ApiMessage body,
                            KrpcFilterContext filterContext) {
        FilterInvoker invoker = responseInvokers.computeIfAbsent(apiKey, (apiKeys -> FilterInvokers.handleNothingInvoker()));
        invoker.onResponse(apiKey, apiVersion, header, body, filterContext);
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
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        FilterInvoker invoker = requestInvokers.computeIfAbsent(apiKey, (apiKeys -> FilterInvokers.handleNothingInvoker()));
        return invoker.shouldHandleRequest(apiKey, apiVersion);
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
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        FilterInvoker invoker = responseInvokers.computeIfAbsent(apiKey, (apiKeys -> FilterInvokers.handleNothingInvoker()));
        return invoker.shouldHandleResponse(apiKey, apiVersion);
    }

}
