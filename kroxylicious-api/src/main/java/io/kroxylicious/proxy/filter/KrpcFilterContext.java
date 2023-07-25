/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

/**
 * A context to allow filters to interact with other filters and the pipeline.
 */
public interface KrpcFilterContext extends BaseKrpcFilterContext, RequestForwardingContext, ResponseForwardingContext {

    RequestForwardingContext deferredForwardRequest();

    ResponseForwardingContext deferredForwardResponse();

}
