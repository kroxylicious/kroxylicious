/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kubernetes;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;

import io.kroxylicious.systemtests.Constants;

public class ClusterRoleBindingTemplates {

    private ClusterRoleBindingTemplates() {
    }

    private static final Logger LOGGER = LogManager.getLogger(ClusterRoleBindingTemplates.class);
    private static final Map<String, String> kroxyLabelSelector = Map.of("app.kubernetes.io/name", "kroxylicious",
            "app.kubernetes.io/component", "operator");

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespaceName) {
        LOGGER.info("Creating ClusterRoleBinding that grant cluster-wide access to all OpenShift projects");
        return clusterRoleBindingsForAllNamespaces(namespaceName, Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME);
    }

    public static List<ClusterRoleBinding> clusterRoleBindingsForAllNamespaces(String namespaceName, String koName) {
        return Arrays.asList(
                getKroxyliciousOperatorDependentCrb(namespaceName, koName),
                getKroxyliciousOperatorFilterGenericCrb(namespaceName, koName),
                getKroxyliciousOperatorWatchedCrb(namespaceName, koName));
    }

    public static ClusterRoleBinding baseClusterRoleBinding(String namespaceName, String name) {
        return new ClusterRoleBindingBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(kroxyLabelSelector)
                .endMetadata()
                .withNewRoleRef()
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind(Constants.CLUSTER_ROLE)
                .withName(name)
                .endRoleRef()
                .withSubjects(new SubjectBuilder()
                        .withKind(Constants.SERVICE_ACCOUNT)
                        .withName(Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME)
                        .withNamespace(namespaceName)
                        .build())
                .build();
    }

    public static ClusterRoleBinding getKroxyliciousOperatorDependentCrb(final String namespaceName, final String koName) {
        return baseClusterRoleBinding(namespaceName, koName + "-dependent");
    }

    public static ClusterRoleBinding getKroxyliciousOperatorFilterGenericCrb(final String namespaceName, final String koName) {
        return baseClusterRoleBinding(namespaceName, koName + "-filter-generic");
    }

    public static ClusterRoleBinding getKroxyliciousOperatorWatchedCrb(final String namespaceName, final String koName) {
        return baseClusterRoleBinding(namespaceName, koName + "-watched");
    }
}
