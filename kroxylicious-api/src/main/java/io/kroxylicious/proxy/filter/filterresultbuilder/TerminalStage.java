/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filterresultbuilder;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.filter.FilterResult;

/**
 * Interface supporting the {@link io.kroxylicious.proxy.filter.FilterResultBuilder} fluent API.
 *
 * @param <FR> filter result
 */
public interface TerminalStage<FR extends FilterResult> {
    /**
     * Constructs the filter result.
     *
     * @return filter result
     */
    FR build();

    /**
     * Produces the filter result contained within a completed {@link CompletionStage}.
     * @return completion stage contain the filter result.
     */
    CompletionStage<FR> completed();
}
