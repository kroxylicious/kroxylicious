/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

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
    static FilterInvoker from(KrpcFilter filter) {
        // all invokers are wrapped in safe invoker so that clients can safely call onRequest/onResponse
        // even if the invoker isn't interested in that message.
        return new SafeInvoker(invokerForFilter(filter));
    }

    private static FilterInvoker invokerForFilter(KrpcFilter filter) {
        boolean isResponseFilter = filter instanceof ResponseFilter;
        boolean isRequestFilter = filter instanceof RequestFilter;
        boolean isAnySpecificFilterInterface = SpecificFilterArrayInvoker.implementsAnySpecificFilterInterface(filter);
        if (isAnySpecificFilterInterface && (isRequestFilter || isResponseFilter)) {
            throw unsupportedFilterInstance(filter, "Cannot mix specific message filter interfaces and [RequestFilter|ResponseFilter] interfaces");
        }
        if (!isRequestFilter && !isResponseFilter && !isAnySpecificFilterInterface) {
            throw unsupportedFilterInstance(filter, "KrpcFilter must implement ResponseFilter, RequestFilter or any combination of specific message Filter interfaces");
        }
        if (isResponseFilter && isRequestFilter) {
            return new RequestResponseInvoker((RequestFilter) filter, (ResponseFilter) filter);
        }
        else if (isRequestFilter) {
            return new RequestFilterInvoker((RequestFilter) filter);
        }
        else if (isResponseFilter) {
            return new ResponseFilterInvoker((ResponseFilter) filter);
        }
        else {
            return arrayInvoker(filter);
        }
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
     * An invoker that does not handle any requests or responses
     * @return invoker
     */
    public static FilterInvoker handleNothingInvoker() {
        return HandleNothingFilterInvoker.INSTANCE;
    }

    private static IllegalArgumentException unsupportedFilterInstance(KrpcFilter filter, String message) {
        return new IllegalArgumentException("Invoker could not be created for: " + filter.getClass().getName() + ". " + message);
    }

}
