/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.systemtests.Constants;

public class KroxyConfigTemplates {

    public static ConfigMapBuilder defaultKroxyConfig(String clusterName, String namespaceName) {
        return new ConfigMapBuilder()
                .withApiVersion("v1")
                .withKind(Constants.CONFIG_MAP_KIND)
                .editMetadata()
                .withName(Constants.KROXY_CONFIG_NAME)
                .withNamespace(namespaceName)
                .endMetadata()
                .addToData("config.yaml", """
                        adminHttp:
                          endpoints:
                            prometheus: {}
                        virtualClusters:
                          demo:
                            targetCluster:
                              bootstrap_servers: %CLUSTER_NAME%-kafka-bootstrap.%NAMESPACE%.svc.cluster.local:9092
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: localhost:9292
                                brokerAddressPattern: %KROXY_SERVICE_NAME%
                            logNetwork: false
                            logFrames: false
                        """
                        .replace("%NAMESPACE%", Constants.KROXY_DEFAULT_NAMESPACE)
                        .replace("%CLUSTER_NAME%", clusterName)
                        .replace("%KROXY_SERVICE_NAME%", Constants.KROXY_SERVICE_NAME));
    }
}
