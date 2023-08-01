/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;

/**
 * Builder for filter results.
 *
 * @param <H>  kafka api message header class
 * @param <FR> filter result
 */
public interface FilterResultBuilder<H extends ApiMessage, FR extends FilterResult> extends CloseStage<FR> {

    CloseOrTerminalStage<FR> forward(H header, ApiMessage message);

    TerminalStage<FR> drop();

}
