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
@ExternalBuildables(editableEnabled = false, generateBuilderPackage = true, builderPackage = BuilderConfig.TARGET_CONFIG_PACKAGE, value = {
        "io.kroxylicious.proxy.config.Configuration",
        "io.kroxylicious.proxy.config.TargetCluster",
        "io.kroxylicious.proxy.config.VirtualCluster",
        "io.kroxylicious.proxy.config.VirtualClusterGateway",
        "io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy",
        "io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy",
        "io.kroxylicious.proxy.config.NamedRange",
        "io.kroxylicious.proxy.config.admin.ManagementConfiguration",
        "io.kroxylicious.proxy.config.admin.EndpointsConfiguration",
        "io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig",
        "io.kroxylicious.proxy.config.secret.FilePassword",
        "io.kroxylicious.proxy.config.secret.InlinePassword",
        "io.kroxylicious.proxy.config.tls.Tls",
        "io.kroxylicious.proxy.config.tls.TrustStore",
        "io.kroxylicious.proxy.config.tls.ServerOptions",
        "io.kroxylicious.proxy.config.tls.InsecureTls",
        "io.kroxylicious.proxy.config.tls.KeyStore",
        "io.kroxylicious.proxy.config.tls.KeyPair",
        "io.kroxylicious.proxy.config.NetworkDefinition",
        "io.kroxylicious.proxy.config.NettySettings"
})
public final class BuilderConfig {
    public static final String TARGET_CONFIG_PACKAGE = "io.kroxylicious.proxy.config.model";

    private BuilderConfig() {
        throw new IllegalStateException();
    }

}
