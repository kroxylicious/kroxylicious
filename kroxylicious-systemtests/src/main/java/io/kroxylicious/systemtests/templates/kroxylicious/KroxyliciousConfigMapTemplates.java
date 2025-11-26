/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.systemtests.Constants;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {

    private KroxyliciousConfigMapTemplates() {
    }

    /**
     * Gets default external kroxylicious config map.
     *
     * @param clusterExternalIP the cluster external ip
     * @return the default external kroxylicious config map
     */
    public static String getDefaultExternalKroxyliciousConfigMap(String clusterExternalIP) {
        return """
                management:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  - name: demo
                    targetCluster:
                      bootstrapServers: %s:9094
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9292
                    logNetwork: false
                    logFrames: false
                """
                .formatted(clusterExternalIP);
    }

    public static ConfigMapBuilder getClusterCaConfigMap(String namespace, String name, String certificate) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of(Constants.KROXYLICIOUS_TLS_CA_NAME, certificate));
        // @formatter:on
    }

    public static ConfigMapBuilder getClusterCaConfigMap(String namespace, String name, Tls tls) {
        if (tls.getTrustAnchorRef() != null) {
            // @formatter:off
            return new ConfigMapBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .endMetadata()
                    .withData(Map.of(Constants.KROXYLICIOUS_TLS_CA_NAME, tls.getTrustAnchorRef().getRef().getName()));
            // @formatter:on
        }
        else {
            return null;
        }
    }

    /**
     * Gets acl rules config map.
     *
     * @param namespace the namespace
     * @param name the name
     * @param aclRules the acl rules
     * @return  the acl rules config map
     */
    public static ConfigMapBuilder getAclRulesConfigMap(String namespace, String name, List<String> aclRules) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of(name, generateAclRules(aclRules)));
        // @formatter:on
    }

    private static String generateAclRules(List<String> aclRules) {
        StringBuilder aclRule = new StringBuilder("from io.kroxylicious.filter.authorization import TopicResource as Topic;");
        aclRules.sort(Collections.reverseOrder());
        aclRules.forEach(rule -> aclRule.append("\n").append(rule));
        aclRule.append("\n").append("otherwise deny;");
        return aclRule.toString();
    }
}
