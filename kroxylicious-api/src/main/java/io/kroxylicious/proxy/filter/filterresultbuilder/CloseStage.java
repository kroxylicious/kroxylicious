/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filterresultbuilder;

import io.kroxylicious.proxy.filter.FilterResult;

public interface CloseStage<FR extends FilterResult> {
    /**
     * Signals the desire of the filter that the connection is closed.
     *
     * @return next builder stage.
     */
    TerminalStage<FR> withCloseConnection();
}
