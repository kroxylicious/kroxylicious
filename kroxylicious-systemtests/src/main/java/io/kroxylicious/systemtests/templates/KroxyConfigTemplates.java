/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.systemtests.Constants;

public class KroxyConfigTemplates {

    public static ConfigMapBuilder defaultKroxyConfig(String namespaceName) {
        return new ConfigMapBuilder()
                .withApiVersion("v1")
                .withKind(Constants.CONFIG_MAP_KIND)
                .editMetadata()
                .withName("kroxylicious-config")
                .withNamespace(namespaceName)
                .endMetadata()
                .addToData("config.yaml", """
                        adminHttp:
                          endpoints:
                            prometheus: {}
                        virtualClusters:
                          demo:
                            targetCluster:
                              bootstrap_servers: my-cluster-kafka-bootstrap:9092
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: localhost:9292
                                brokerAddressPattern: kroxylicious-service
                            logNetwork: false
                            logFrames: false
                        """);
    }
}
