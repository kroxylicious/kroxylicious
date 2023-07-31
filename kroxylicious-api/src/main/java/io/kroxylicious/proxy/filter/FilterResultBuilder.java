/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Builder for filter results.
 *
 * @param <FRB> concrete filter result builder
 * @param <FR> concrete filter result
 */
public interface FilterResultBuilder<FRB extends FilterResultBuilder<FRB, FR>, FR extends FilterResult> {
    FRB withHeader(ApiMessage header);

    FRB withMessage(ApiMessage message);

    FRB withCloseConnection(boolean closeConnection);

    FR build();

    CompletionStage<FR> completedFilterResult();

}
