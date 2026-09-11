/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.operator.v1.IngressController;
import io.fabric8.openshift.api.model.operator.v1.IngressControllerStatus;

/**
 * OpenShift platform utilities for operator integration tests.
 */
public final class OpenShiftUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenShiftUtils.class);
    public static final String DEFAULT_INGRESS_CONTROLLER_DOMAIN = "apps.crc.testing";

    private OpenShiftUtils() {
    }

    /**
     * Returns {@code true} if the connected Kubernetes cluster supports the OpenShift {@link Route} resource kind.
     */
    public static boolean supportsRoute() {
        try (var client = new KubernetesClientBuilder().build()) {
            return client.supports(Route.class);
        }
    }

    /**
     * Gets the domain for the default OpenShift ingress controller.
     * <p>
     * Real OpenShift has the IngressController API and an instance called {@code default}
     * in the {@code openshift-ingress-operator} namespace. MicroShift has the API but no
     * instances or namespace. Minikube has neither. The Fabric8 {@code #get} API treats
     * absence of resource/namespace/API in the same manner (all return {@code null}).
     * Falls back to {@link #DEFAULT_INGRESS_CONTROLLER_DOMAIN} when detection fails.
     */
    public static String getDefaultIngressControllerDomain() {
        IngressController defaultIngressController;
        try (var client = new KubernetesClientBuilder().build()) {
            defaultIngressController = client.resources(IngressController.class)
                    .inNamespace("openshift-ingress-operator").withName("default").get();
        }

        return Optional.ofNullable(defaultIngressController)
                .map(IngressController::getStatus)
                .map(IngressControllerStatus::getDomain)
                .orElseGet(() -> {
                    LOGGER.warn("Couldn't programmatically determine OpenShiftIngressController domain, using test default: {}", DEFAULT_INGRESS_CONTROLLER_DOMAIN);
                    return DEFAULT_INGRESS_CONTROLLER_DOMAIN;
                });
    }
}
