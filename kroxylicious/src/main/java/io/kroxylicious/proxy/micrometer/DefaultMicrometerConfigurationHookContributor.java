/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.service.BaseContributor;

public class DefaultMicrometerConfigurationHookContributor extends BaseContributor<MicrometerConfigurationHook> implements MicrometerConfigurationHookContributor {

    public static final BaseContributorBuilder<MicrometerConfigurationHook> BUILDER = BaseContributor.<MicrometerConfigurationHook> builder()
            .add("CommonTags", CommonTagsHook.CommonTagsHookConfig.class, CommonTagsHook::new)
            .add("StandardBinders", StandardBindersHook.StandardBindersHookConfig.class, StandardBindersHook::new);

    public DefaultMicrometerConfigurationHookContributor() {
        super(BUILDER);
    }
}
