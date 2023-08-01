/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

public interface RequestFilterResultBuilder extends FilterResultBuilder<RequestHeaderData, RequestFilterResultBuilder, RequestFilterResult> {

    CloseStage<RequestFilterResult> shortCircuitResponse(ResponseHeaderData header, ApiMessage message);

    CloseStage<RequestFilterResult> shortCircuitResponse(ApiMessage message);

    RequestFilterResultBuilder asRequestShortCircuitResponse();

}
