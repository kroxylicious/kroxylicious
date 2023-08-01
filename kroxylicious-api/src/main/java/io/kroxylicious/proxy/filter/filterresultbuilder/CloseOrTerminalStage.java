/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filterresultbuilder;

import io.kroxylicious.proxy.filter.FilterResult;

public interface CloseOrTerminalStage<FR extends FilterResult> extends TerminalStage<FR>, CloseStage<FR> {
}
