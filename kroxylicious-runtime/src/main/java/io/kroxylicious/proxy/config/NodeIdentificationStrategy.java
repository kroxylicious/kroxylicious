/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.function.Supplier;

public interface NodeIdentificationStrategy extends Supplier<ClusterNetworkAddressConfigProviderDefinition> {
}
