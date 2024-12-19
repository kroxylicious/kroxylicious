/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import io.kroxylicious.systemtests.Constants;

public class KroxyliciousServiceTemplates {

    private static final Map<String, String> kroxyLabelSelector = Map.of("app", "kroxylicious");

    public static ServiceBuilder defaultKroxyService(String namespaceName) {
        return new ServiceBuilder()
                .withApiVersion("v1")
                .withKind(Constants.SERVICE_KIND)
                .withNewMetadata()
                .withName(Constants.KROXY_SERVICE_NAME)
                .withNamespace(namespaceName)
                .endMetadata()
                .editSpec()
                .withSelector(kroxyLabelSelector)
                .withPorts(getPlainServicePorts())
                .endSpec();
    }

    private static List<ServicePort> getPlainServicePorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(createServicePort("port-9292", 9292, 9292));
        servicePorts.add(createServicePort("port-9293", 9293, 9293));
        servicePorts.add(createServicePort("port-9294", 9294, 9294));
        servicePorts.add(createServicePort("port-9295", 9295, 9295));
        return servicePorts;
    }

    private static ServicePort createServicePort(String name, int port, int targetPort) {
        return new ServicePortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withPort(port)
                .withTargetPort(new IntOrString(targetPort))
                .build();
    }
}
