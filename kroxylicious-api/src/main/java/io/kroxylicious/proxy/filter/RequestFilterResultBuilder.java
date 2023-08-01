/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;

public interface RequestFilterResultBuilder extends FilterResultBuilder<RequestHeaderData, RequestFilterResultBuilder, RequestFilterResult> {

    RequestFilterResultBuilder asRequestShortCircuitResponse();

}
