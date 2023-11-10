/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.proxy.service.Contributor;

public interface MicrometerConfigurationHookContributor<B> extends Contributor<MicrometerConfigurationHook, B, Context<B>> {
}
