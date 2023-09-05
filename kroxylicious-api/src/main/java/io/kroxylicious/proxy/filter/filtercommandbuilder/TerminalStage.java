/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filtercommandbuilder;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.filter.FilterCommand;

/**
 * Interface supporting the {@link io.kroxylicious.proxy.filter.FilterCommandBuilder} fluent API.
 *
 * @param <R> filter result
 */
public interface TerminalStage<R extends FilterCommand> {
    /**
     * Constructs the filter result.
     *
     * @return filter result
     */
    R build();

    /**
     * Produces the filter result contained within a completed {@link CompletionStage}.
     * @return completion stage contain the filter result.
     */
    CompletionStage<R> completed();
}
