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

    /**
     * Create a FilterInvoker for this filter. Supported cases are:
     * <ol>
     *     <li>A KrpcFilter implementing {@link ResponseFilter}</li>
     *     <li>A KrpcFilter implementing {@link RequestFilter}</li>
     *     <li>A KrpcFilter implementing both {@link ResponseFilter} and {@link RequestFilter} </li>
     *     <li>A KrpcFilter implementing qny number of Specific Message Filter interfaces</li>
     * </ol>
     * @throws IllegalArgumentException if specific Message Filter interfaces are mixed with {@link RequestFilter} or  {@link ResponseFilter}
     * @throws IllegalArgumentException if none of the supported interfaces are implemented
     * @param filter the Filter to create an invoker for
     * @return the invoker
     */
    public static FilterInvoker from(KrpcFilter filter) {
        boolean isResponseFilter = filter instanceof ResponseFilter;
        boolean isRequestFilter = filter instanceof RequestFilter;
        boolean isAnySpecificFilterInterface = SpecificFilterInvoker.implementsAnySpecificFilterInterface(filter);
        if (isAnySpecificFilterInterface && (isRequestFilter || isResponseFilter)) {
            throw new IllegalArgumentException("Cannot mix specific message filter interfaces and all message filter interfaces");
        }
        if (!isRequestFilter && !isResponseFilter && !isAnySpecificFilterInterface) {
            throw new IllegalArgumentException("KrpcFilter must implement ResponseFilter, RequestFilter or any combination of specific message Filter interfaces");
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
            return new SpecificFilterInvoker(filter);
        }
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
    public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filter.onRequest(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return filter.shouldHandleRequest(apiKey, apiVersion);
    }

    }

    private record ResponseFilterInvoker(ResponseFilter filter) implements FilterInvoker {

    @Override
    public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        filter.onResponse(apiKey, header, body, filterContext);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return filter.shouldHandleResponse(apiKey, apiVersion);
    }

    }

    private record RequestResponseInvoker(RequestFilter requestFilter, ResponseFilter responseFilter) implements FilterInvoker {

    @Override
    public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        requestFilter.onRequest(apiKey, header, body, filterContext);
    }

    @Override
    public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
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

}
