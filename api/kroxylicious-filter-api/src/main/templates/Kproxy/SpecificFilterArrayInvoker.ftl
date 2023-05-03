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

import java.util.Arrays;
import java.util.OptionalInt;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Invoker for KrpcFilters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
class SpecificFilterArrayInvoker implements FilterInvoker {

    private final FilterInvoker[] requestInvokers;
    private final FilterInvoker[] responseInvokers;

    SpecificFilterArrayInvoker(KrpcFilter filter) {
        OptionalInt maybeMaxId = Arrays.stream(ApiMessageType.values()).mapToInt(ApiMessageType::apiKey).max();
        if (maybeMaxId.isEmpty()) {
            throw new IllegalStateException("no maximum id found");
        }
        int arraySize = maybeMaxId.getAsInt() + 1;
        this.requestInvokers = new FilterInvoker[arraySize];
        this.responseInvokers = new FilterInvoker[arraySize];
        <#list messageSpecs as messageSpec>
        <#if messageSpec.type?lower_case == 'request'>
        this.requestInvokers[${messageSpec.apiKey.get()}] = filter instanceof ${messageSpec.name}Filter ? new ${messageSpec.name}FilterInvoker((${messageSpec.name}Filter) filter) : null;
        </#if>
        </#list>
        <#list messageSpecs as messageSpec>
        <#if messageSpec.type?lower_case == 'response'>
        this.responseInvokers[${messageSpec.apiKey.get()}] = filter instanceof ${messageSpec.name}Filter ? new ${messageSpec.name}FilterInvoker((${messageSpec.name}Filter) filter) : null;
        </#if>
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
        FilterInvoker invoker = getInvoker(requestInvokers, apiKey);
        if (invoker == null) {
            filterContext.forwardRequest(header, body);
        }
        else {
            invoker.onRequest(apiKey, apiVersion, header, body, filterContext);
        }
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
        FilterInvoker invoker = getInvoker(responseInvokers, apiKey);
        if (invoker == null) {
            filterContext.forwardResponse(header, body);
        }
        else {
            invoker.onResponse(apiKey, apiVersion, header, body, filterContext);
        }
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
        FilterInvoker invoker = getInvoker(requestInvokers, apiKey);
        if (invoker == null) {
            return false;
        }
        else {
            return invoker.shouldHandleRequest(apiKey, apiVersion);
        }
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
        FilterInvoker invoker = getInvoker(responseInvokers, apiKey);
        if (invoker == null) {
            return false;
        }
        else {
            return invoker.shouldHandleResponse(apiKey, apiVersion);
        }
    }

    private FilterInvoker getInvoker(FilterInvoker[] invokers, ApiKeys apiKey) {
        if (apiKey.id >= invokers.length) {
            return null;
        }
        return invokers[apiKey.id];
    }

}
