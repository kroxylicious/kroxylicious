/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.service.Contributor;

/**
 * FilterContributor acts source of kroxylicious filter implementations.
 * Users wishing to provide filters  must implement this class and register the service on the
 * classpath. See {@link java.util.ServiceLoader} for details.
 */
public interface FilterContributor extends Contributor<KrpcFilter> {
}
