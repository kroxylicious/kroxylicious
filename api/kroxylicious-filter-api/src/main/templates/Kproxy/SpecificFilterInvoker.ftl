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

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Invoker for KrpcFilters that implement any number of Specific Message interfaces (for
 * example {@link io.kroxylicious.proxy.filter.AlterConfigsResponseFilter}.
 */
class SpecificFilterInvoker implements FilterInvoker {

    private final KrpcFilter filter;

    SpecificFilterInvoker(KrpcFilter filter) {
        this.filter = filter;
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
        switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} -> ((${messageSpec.name}Filter) filter).on${messageSpec.name}(header, (${messageSpec.name}Data) body, filterContext);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
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
        switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
            case ${retrieveApiKey(messageSpec)} -> ((${messageSpec.name}Filter) filter).on${messageSpec.name}(header, (${messageSpec.name}Data) body, filterContext);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported RPC " + apiKey);
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
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} -> filter instanceof ${messageSpec.name}Filter && ((${messageSpec.name}Filter) filter).shouldHandle${messageSpec.name}(apiVersion);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
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
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
            case ${retrieveApiKey(messageSpec)} -> filter instanceof ${messageSpec.name}Filter && ((${messageSpec.name}Filter) filter).shouldHandle${messageSpec.name}(apiVersion);
    </#if>
</#list>
            default -> throw new IllegalStateException("Unsupported API key " + apiKey);
        };
    }

    /**
    * Check if a KrpcFilter implements any of the Specific Message Filter interfaces
    * @param filter the filter
    * @return true if the filter implements any Specific Message Filter interfaces
    */
    public static boolean implementsAnySpecificFilterInterface(KrpcFilter filter) {
        return <#list messageSpecs as messageSpec>filter instanceof ${messageSpec.name}Filter<#sep> ||
                </#sep></#list>;
    }

}
