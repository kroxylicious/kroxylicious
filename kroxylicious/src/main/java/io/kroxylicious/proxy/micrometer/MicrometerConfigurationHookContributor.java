/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.service.Contributor;
import io.kroxylicious.proxy.service.ContributorContext;

public interface MicrometerConfigurationHookContributor extends Contributor<MicrometerConfigurationHook, ContributorContext> {
}
