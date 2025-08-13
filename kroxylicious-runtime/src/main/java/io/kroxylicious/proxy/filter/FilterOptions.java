/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * Options for a Filter
 * @param filterName name of the filter
 * @param targetMessageClass what class of messages to intercept
 */
public record FilterOptions(String filterName, TargetMessageClass targetMessageClass) {}
