/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Factory for FilterInvokers. The intention is to keep the Invoker implementations
 * as private as we can, so that invocation is a framework concern.
 */
public class FilterInvokers {

    private FilterInvokers() {

    }

    /**
     * Create a FilterInvoker for this filter. Supported cases are:
     * <ol>
     *     <li>A KrpcFilter implementing {@link ResponseFilter}</li>
     *     <li>A KrpcFilter implementing {@link RequestFilter}</li>
     *     <li>A KrpcFilter implementing both {@link ResponseFilter} and {@link RequestFilter} </li>
     *     <li>A KrpcFilter implementing any number of Specific Message Filter interfaces</li>
     * </ol>
     * @throws IllegalArgumentException if specific Message Filter interfaces are mixed with {@link RequestFilter} or  {@link ResponseFilter}
     * @throws IllegalArgumentException if none of the supported interfaces are implemented
     * @param filter the Filter to create an invoker for
     * @return the invoker
     */
    public static FilterInvoker from(KrpcFilter filter) {
        return new SafeInvoker(invokerForFilter(filter));
    }

    private static FilterInvoker invokerForFilter(KrpcFilter filter) {
        boolean isResponseFilter = filter instanceof ResponseFilter;
        boolean isRequestFilter = filter instanceof RequestFilter;
        boolean isAnySpecificFilterInterface = SpecificFilterInvoker.implementsAnySpecificFilterInterface(filter);
        if (isAnySpecificFilterInterface && (isRequestFilter || isResponseFilter)) {
            throw unsupportedFilterInstance(filter, "Cannot mix specific message filter interfaces and [RequestFilter|ResponseFilter] interfaces");
        }
        if (!isRequestFilter && !isResponseFilter && !isAnySpecificFilterInterface) {
            throw unsupportedFilterInstance(filter, "KrpcFilter must implement ResponseFilter, RequestFilter or any combination of specific message Filter interfaces");
        }
        if (isResponseFilter && isRequestFilter) {
            return requestResponseInvoker((RequestFilter) filter, (ResponseFilter) filter);
        }
        else if (isRequestFilter) {
            return requestInvoker((RequestFilter) filter);
        }
        else if (isResponseFilter) {
            return responseInvoker((ResponseFilter) filter);
        }
        else {
            return arrayInvoker2(filter);
        }
    }

    /**
     * Create an invoker for this filter that uses instanceof when deciding
     * if the filter should be consulted/handle messages.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker instanceOfInvoker(KrpcFilter filter) {
        return new SpecificFilterInvoker(filter);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it stores
     * an invoker for each targeted request-type and response-type in an array.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker arrayInvoker(KrpcFilter filter) {
        return new SpecificFilterArrayInvoker(filter);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it stores
     * an invoker for each targeted request-type and response-type in an array.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker arrayInvoker2(KrpcFilter filter) {
        return new SpecificFilterArrayInvoker2(filter);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it has a field
     * for each specific filter type and populates them at construction time if the
     * filter matches those types.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker fieldInvoker(KrpcFilter filter) {
        return new SpecificFilterFieldInvoker(filter);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it has a map
     * containing invokers for each specific filter type and populates them at
     * construction time if the filter matches those types.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker mapInvoker(KrpcFilter filter) {
        return new SpecificFilterMapInvoker(filter);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it has a map
     * containing invokers for each specific filter type and populates them at
     * construction time if the filter matches those types.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker mapInvoker2(KrpcFilter filter) {
        return new SpecificFilterMapInvoker2(filter);
    }

    private static IllegalArgumentException unsupportedFilterInstance(KrpcFilter filter, String message) {
        return new IllegalArgumentException("Invoker could not be created for: " + filter.getClass().getName() + ". " + message);
    }

    public static FilterInvoker handleNothingInvoker() {
        return new FilterInvoker() {

            @Override
            public void onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
                throw new IllegalStateException("I should never be invoked");
            }

            @Override
            public void onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
                throw new IllegalStateException("I should never be invoked");
            }
        };
    }

    private static FilterInvoker requestInvoker(RequestFilter filter) {
        return new RequestFilterInvoker(filter);
    }

    private static FilterInvoker responseInvoker(ResponseFilter filter) {
        return new ResponseFilterInvoker(filter);
    }

    private static FilterInvoker requestResponseInvoker(RequestFilter requestFilter, ResponseFilter responseFilter) {
        return new RequestResponseInvoker(requestFilter, responseFilter);
    }

    private record RequestFilterInvoker(RequestFilter filter) implements FilterInvoker {

    @Override
    public void onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filter.onRequest(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return filter.shouldHandleRequest(apiKey, apiVersion);
    }

    }

    private record ResponseFilterInvoker(ResponseFilter filter) implements FilterInvoker {

    @Override
    public void onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filter.onResponse(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return filter.shouldHandleResponse(apiKey, apiVersion);
    }

    }

    private record RequestResponseInvoker(RequestFilter requestFilter, ResponseFilter responseFilter) implements FilterInvoker {

    @Override
    public void onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        requestFilter.onRequest(apiKey, header, body, filterContext);
    }

    @Override
    public void onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        responseFilter.onResponse(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return requestFilter.shouldHandleRequest(apiKey, apiVersion);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return responseFilter.shouldHandleResponse(apiKey, apiVersion);
    }

    }

    private record SafeInvoker(FilterInvoker invoker) implements FilterInvoker {

    @Override
    public void onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (invoker.shouldHandleRequest(apiKey, apiVersion)) {
            invoker.onRequest(apiKey, apiVersion, header, body, filterContext);
        }
        else {
            filterContext.forwardRequest(header, body);
        }
    }

    @Override
    public void onResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (invoker.shouldHandleResponse(apiKey, apiVersion)) {
            invoker.onResponse(apiKey, apiVersion, header, body, filterContext);
        }
        else {
            filterContext.forwardResponse(header, body);
        }
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return invoker.shouldHandleRequest(apiKey, apiVersion);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return invoker.shouldHandleResponse(apiKey, apiVersion);
    }
}

}
