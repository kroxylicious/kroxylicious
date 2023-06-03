/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.sundr.builder.annotations.ExternalBuildables;

/**
 * This class exists to configure Sundrio so that builders are generated for the configuration model.
 */
@ExternalBuildables(editableEnabled = false, generateBuilderPackage = true, builderPackage = BuilderConfig.IO_KROXYLICIOUS_PROXY_CONFIG, value = {
        "io.kroxylicious.proxy.config.Configuration",
        "io.kroxylicious.proxy.config.TargetCluster",
        "io.kroxylicious.proxy.config.VirtualCluster",
        "io.kroxylicious.proxy.config.admin.AdminHttpConfiguration",
        "io.kroxylicious.proxy.config.admin.EndpointsConfiguration",
        "io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig" })
public final class BuilderConfig {
    public static final String IO_KROXYLICIOUS_PROXY_CONFIG = "io.kroxylicious.proxy.config";

    private BuilderConfig() {
        throw new IllegalStateException();
    }

}
