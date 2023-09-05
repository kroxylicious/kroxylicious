/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.filtercommandbuilder;

import io.kroxylicious.proxy.filter.FilterCommand;

/**
 * Interface supporting the {@link io.kroxylicious.proxy.filter.FilterCommandBuilder} fluent API.
 *
 * @param <R> filter result
 */
public interface CloseOrTerminalStage<R extends FilterCommand> extends TerminalStage<R>, CloseStage<R> {
}
