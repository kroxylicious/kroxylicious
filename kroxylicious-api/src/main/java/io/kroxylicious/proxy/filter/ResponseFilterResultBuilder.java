/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.ResponseHeaderData;

/**
 * Builder for response filter results.
 */
public interface ResponseFilterResultBuilder extends FilterResultBuilder<ResponseHeaderData, ResponseFilterResult> {
}
