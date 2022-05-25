<#--

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements. See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

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
import org.apache.kafka.common.protocol.ApiKeys;

import io.strimzi.kproxy.codec.DecodedResponseFrame;

/**
 * <p>Interface for {@code *ResponseFilter}s.
 * This interface can be implemented in two ways:
 * <ul>
 *     <li>filter classes can (multiply) implement one of the RPC-specific subinterfaces such as {@link ProduceResponseFilter} for a type-safe API</li>
 *     <li>filter classes can extend {@link KrpcGenericResponseFilter}</li>
 * </ul>
 *
 * <p>When implementing one or more of the {@code *ResponseFilter} subinterfaces you need only implement
 * the {@code on*Response} method(s), unless your filter can avoid deserialization in which case
 * you can override {@link #shouldDeserializeResponse(ApiKeys, short)} as well.</p>
 *
 * <p>When extending {@link KrpcGenericResponseFilter} you need to override {@link #apply(DecodedResponseFrame, KrpcFilterContext)},
 * and may override {@link #shouldDeserializeResponse(ApiKeys, short)} as well.</p>
 *
 * <h3>Guarantees</h3>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the filter is associated with a single channel</li>
 *     <li>That {@link #shouldDeserializeResponse(ApiKeys, short)} and
 *     {@link #apply(DecodedResponseFrame, KrpcFilterContext)} (or {@code on*Response} as appropriate)
 *     will always be invoked on the same thread.</li>
 *     <li>That filters are applied in the order they were configured.</li>
 * </ol>
 * <p>From 1. and 2. it follows that you can use member variables in your filter to
 * store channel-local state.</p>
 *
 * <p>Implementors should <strong>not</strong> assume:</p>
 * <ol>
 *     <li>That filters in the same chain execute on the same thread. Thus inter-filter communication/state
 *     transfer needs to be thread-safe</li>
 * </ol>
 */
public /* sealed */ interface KrpcResponseFilter extends KrpcFilter /* TODO permits ... */ {

    /**
     * Apply the filter to the given {@code decodedFrame} using the given {@code filterContext}.
     * @param decodedFrame The response frame.
     * @param filterContext The filter context.
     * @return The state of the filter.
     */
    public default KrpcFilterState apply(DecodedResponseFrame<?> decodedFrame,
                                         KrpcFilterContext filterContext) {
        KrpcFilterState state;
        switch (decodedFrame.apiKey()) {
<#list messageSpecs as messageSpec>
            case ${retrieveApiKey(messageSpec)}:
                state = ((${messageSpec.name}Filter) this).on${messageSpec.name}((${messageSpec.name}Data) decodedFrame.body(), filterContext);
                break;
</#list>
            default:
                throw new IllegalStateException("Unsupported RPC " + decodedFrame.apiKey());
        }
        return state;
    }

    /**
     * <p>Determines whether a response with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per response,
     * or that two consecutive calls refer to the same response.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldDeserializeResponse} on response A</li>
     *     <li>{@code shouldDeserializeResponse} on response B</li>
     *     <li>{@code shouldDeserializeResponse} on response A</li>
     *     <li>{@code apply} on response A</li>
     *     <li>{@code apply} on response B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return
     */
    default boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
        switch (apiKey) {
<#list messageSpecs as messageSpec>
            case ${retrieveApiKey(messageSpec)}:
                return this instanceof ${messageSpec.name}Filter;
</#list>
            default:
                throw new IllegalStateException("Unsupported API key " + apiKey);
        }
    }
}
