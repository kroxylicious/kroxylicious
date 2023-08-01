/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filterresultbuilder;

import io.kroxylicious.proxy.filter.FilterResult;

/**
 * Interface supporting the {@link io.kroxylicious.proxy.filter.FilterResultBuilder} fluent API.
 *
 * @param <FR> filter result
 */
public interface CloseStage<FR extends FilterResult> {
    /**
     * Signals the desire of the filter that the connection is closed.
     *
     * @return next stage in the fluent API.
     */
    TerminalStage<FR> withCloseConnection();
}
