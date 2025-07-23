/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
     *     <li>A Filter implementing {@link ResponseFilter}</li>
     *     <li>A Filter implementing {@link RequestFilter}</li>
     *     <li>A Filter implementing both {@link ResponseFilter} and {@link RequestFilter} </li>
     *     <li>A Filter implementing any number of Specific Message Filter interfaces</li>
     * </ol>
     * Examples of unsupported cases are:
     * <ol>
     *     <li>A Filter implementing {@link ResponseFilter} and any number of Specific Message Filter interfaces</li>
     * </ol>
     * @throws IllegalArgumentException if there is an invalid combination of Filter interfaces
     * @throws IllegalArgumentException if none of the supported interfaces are implemented
     * @param filter the Filter to create an invoker for
     * @return the invoker
     */
    static List<FilterAndInvoker> from(String filterName, Filter filter) {
        List<FilterAndInvoker> filterInvokers = invokersForFilter(filterName, filter);
        // all invokers are wrapped in safe invoker so that clients can safely call onRequest/onResponse
        // even if the invoker isn't interested in that message.
        return wrapAllInSafeInvoker(filterInvokers).toList();
    }

    private static List<FilterAndInvoker> invokersForFilter(String filterName, Filter filter) {
        FilterCharacteristics characteristics = FilterCharacteristics.describeCharacteristics(filter);
        validateFilter(filterName, characteristics);
        Optional<FilterAndInvoker> genericInvoker = maybeGenericInvoker(filterName, filter, characteristics);
        Optional<FilterAndInvoker> specificInvoker = maybeSpecificInvoker(filterName, filter, characteristics);
        return Stream.concat(genericInvoker.stream(), specificInvoker.stream()).toList();
    }

    private static Optional<FilterAndInvoker> maybeSpecificInvoker(String filterName, Filter filter, FilterCharacteristics characteristics) {
        if (characteristics.isSpecificRequestFilter || characteristics.isSpecificResponseFilter) {
            return Optional.of(getFilterAndInvoker(filterName, filter, arrayInvoker(filter)));
        }
        else {
            return Optional.empty();
        }
    }

    private static Optional<FilterAndInvoker> maybeGenericInvoker(String filterName, Filter filter, FilterCharacteristics characteristics) {
        if (characteristics.isAnyRequestFilter && characteristics.isAnyResponseFilter) {
            return Optional.of(getFilterAndInvoker(filterName, filter, new RequestResponseInvoker((RequestFilter) filter, (ResponseFilter) filter)));
        }
        else if (characteristics.isAnyResponseFilter) {
            return Optional.of(getFilterAndInvoker(filterName, filter, new ResponseFilterInvoker((ResponseFilter) filter)));
        }
        else if (characteristics.isAnyRequestFilter) {
            return Optional.of(getFilterAndInvoker(filterName, filter, new RequestFilterInvoker((RequestFilter) filter)));
        }
        return Optional.empty();
    }

    private static Stream<FilterAndInvoker> wrapAllInSafeInvoker(List<FilterAndInvoker> filterInvokers) {
        return filterInvokers.stream()
                .map(filterAndInvoker -> getFilterAndInvoker(filterAndInvoker.filterName(), filterAndInvoker.filter(), new SafeInvoker(filterAndInvoker.invoker())));
    }

    private static void validateFilter(String filterName, FilterCharacteristics characteristics) {
        if (characteristics.isAnyRequestFilter
                && characteristics.isSpecificRequestFilter) {
            throw unsupportedFilterInstance(filterName, "Cannot mix specific request message filter interfaces and RequestFilter interfaces");
        }
        if (characteristics.isAnyResponseFilter
                && characteristics.isSpecificResponseFilter) {
            throw unsupportedFilterInstance(filterName, "Cannot mix specific response message filter interfaces and ResponseFilter interfaces");
        }
        if (!characteristics.isAnyRequestFilter
                && !characteristics.isAnyResponseFilter
                && !characteristics.isSpecificRequestFilter
                && !characteristics.isSpecificResponseFilter) {
            throw unsupportedFilterInstance(filterName,
                    "Filter must implement ResponseFilter, RequestFilter or any combination of specific message Filter interfaces");
        }
    }

    private static FilterAndInvoker getFilterAndInvoker(String filterName, Filter filter, FilterInvoker invoker) {
        return new FilterAndInvoker(filterName, filter, invoker);
    }

    /**
     * Create an invoker for this filter that avoids instanceof when deciding
     * if the filter should be consulted/handle messages. Instead, it stores
     * an invoker for each targeted request-type and response-type in an array.
     * @param filter the filter
     * @return an invoker for the filter
     */
    public static FilterInvoker arrayInvoker(Filter filter) {
        return new SpecificFilterArrayInvoker(filter);
    }

    /**
     * An invoker that does not handle any requests or responses
     * @return invoker
     */
    public static FilterInvoker handleNothingInvoker() {
        return HandleNothingFilterInvoker.INSTANCE;
    }

    private static IllegalArgumentException unsupportedFilterInstance(String filterName, String message) {
        return new IllegalArgumentException("Invoker could not be created for filter: " + filterName + ". " + message);
    }

    private record FilterCharacteristics(boolean isAnyRequestFilter, boolean isAnyResponseFilter, boolean isSpecificRequestFilter, boolean isSpecificResponseFilter) {
        static FilterCharacteristics describeCharacteristics(Filter filter) {
            boolean isAnyRequestFilter = filter instanceof RequestFilter;
            boolean isSpecificRequestFilter = SpecificFilterArrayInvoker.implementsAnySpecificRequestFilterInterface(filter);
            boolean isAnyResponseFilter = filter instanceof ResponseFilter;
            boolean isSpecificResponseFilter = SpecificFilterArrayInvoker.implementsAnySpecificResponseFilterInterface(filter);
            return new FilterCharacteristics(isAnyRequestFilter, isAnyResponseFilter, isSpecificRequestFilter, isSpecificResponseFilter);
        }
    }

}
