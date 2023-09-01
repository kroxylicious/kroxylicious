/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.templates.kubernetes;

import java.util.Collections;

import io.fabric8.kubernetes.api.model.ServiceBuilder;

public class ServiceTemplates {

    public static ServiceBuilder getSystemtestsServiceResource(String appName, int port, String namespace, String transportProtocol) {
        return new ServiceBuilder()
                .withNewMetadata()
                .withName(appName)
                .withNamespace(namespace)
                .addToLabels("run", appName)
                .endMetadata()
                .withNewSpec()
                .withSelector(Collections.singletonMap("app", appName))
                .addNewPort()
                .withName("http")
                .withPort(port)
                .withProtocol(transportProtocol)
                .endPort()
                .endSpec();
    }
}
