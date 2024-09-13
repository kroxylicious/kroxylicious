<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
====
    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version2.0,available at http://www.apache.org/licenses/LICENSE-2.0
====

package ${outputPackage};

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.codec.DecodedRequestFrame;

/**
 * <p>Interface for {@code *RequestFilter}s.
 * This interface can be implemented in two ways:
 * <ul>
 *     <li>filter classes can (multiply) implement one of the RPC-specific subinterfaces such as {@link ProduceRequestFilter} for a type-safe API</li>
 *     <li>filter classes can extend {@link KrpcGenericRequestFilter}</li>
 * </ul>
 *
 * <p>When implementing one or more of the {@code *RequestFilter} subinterfaces you need only implement
 * the {@code on*Request} method(s), unless your filter can avoid deserialization in which case
 * you can override {@link #shouldDeserializeRequest(ApiKeys, short)} as well.</p>
 *
 * <p>When extending {@link KrpcGenericRequestFilter} you need to override {@link #apply(DecodedRequestFrame, KrpcFilterContext)},
 * and may override {@link #shouldDeserializeRequest(ApiKeys, short)} as well.</p>
 *
 * <h2>Guarantees</h2>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the filter is associated with a single channel</li>
 *     <li>That {@link #shouldDeserializeRequest(ApiKeys, short)} and
 *     {@link #apply(DecodedRequestFrame, KrpcFilterContext)} (or {@code on*Request} as appropriate)
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
public /* sealed */ interface KrpcRequestFilter extends KrpcFilter /* TODO permits ... */ {

    /**
     * Apply the filter to the given {@code decodedFrame} using the given {@code filterContext}.
     * @param decodedFrame The request frame.
     * @param filterContext The filter context.
     * @return The state of the filter.
     */
    public default KrpcFilterState apply(
            DecodedRequestFrame<?> decodedFrame,
            KrpcFilterContext filterContext) {
        KrpcFilterState state;
        switch (decodedFrame.apiKey()) {
<#list messageSpecs as messageSpec>
            case ${retrieveApiKey(messageSpec)}:
                state = ((${messageSpec.name}Filter) this).on${messageSpec.name}(
                        (${messageSpec.name}Data) decodedFrame.body(),
                        filterContext);
                break;
</#list>
            default:
                throw new IllegalStateException("Unsupported RPC " + decodedFrame.apiKey());
        }
        return state;
    }

    /**
     * <p>Determines whether a request with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * Note that it is not guaranteed that this method will be called once per request,
     * or that two consecutive calls refer to the same request.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldDeserializeRequest} on request A</li>
     *     <li>{@code shouldDeserializeRequest} on request B</li>
     *     <li>{@code shouldDeserializeRequest} on request A</li>
     *     <li>{@code apply} on request A</li>
     *     <li>{@code apply} on request B</li>
     * </ol>
     * @param apiKey The API key
     * @param apiVersion The API version
     * @return
     */
    default boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
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
