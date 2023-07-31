/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

public interface RequestFilterResultBuilder extends FilterResultBuilder<RequestFilterResultBuilder, RequestFilterResult> {

    RequestFilterResultBuilder asRequestShortCircuitResponse();

}
