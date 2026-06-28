/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.KafSaslConfig;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {
    private static final ObjectMapper OBJECT_MAPPER = new YAMLMapper();

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
        aclRules.sort(Collections.reverseOrder()); // deny should be always first, then allow sentences
        aclRules.forEach(rule -> aclRule.append("\n").append(rule));
        aclRule.append("\n").append("otherwise deny;");
        return aclRule.toString();
    }

    /**
     * Gets config map for kaf config.
     *
     * @param namespace the namespace
     * @param name the name
     * @param bootstrap the bootstrap
     * @param additionalConfig the additional config
     * @return  the config map for kaf config
     */
    public static ConfigMapBuilder getConfigMapForKafConfig(String namespace, String name, String bootstrap, Map<String, String> additionalConfig) {

        KafSaslConfig kafSaslConfig = null;
        if (additionalConfig.containsKey(SaslConfigs.SASL_MECHANISM)) {
            kafSaslConfig = new KafSaslConfig(
                    additionalConfig.get(SaslConfigs.SASL_MECHANISM),
                    additionalConfig.get("sasl.username"),
                    additionalConfig.get("sasl.password"),
                    1);
        }

        Map<String, Object> saslConfigMap = OBJECT_MAPPER
                .convertValue(kafSaslConfig, new TypeReference<>() {
                });

        String config = "current-cluster: local"
                + "\n" + "clusters:"
                + "\n" + "- name: local"
                + "\n" + "  brokers:"
                + "\n" + "  - " + bootstrap
                + "\n" + "  SASL: " + getSaslConfigMap(saslConfigMap)
                + "\n" + "  TLS: " + "null"
                + "\n" + "  security-protocol: \"" + additionalConfig.getOrDefault(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "") + "\""
                + "\n" + "  schema-registry-url: \"\""
                + "\n" + "  schema-registry-credentials: null";

        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of(Constants.KAF_CONFIG_FILE_NAME, config));
        // @formatter:on
    }

    private static String getSaslConfigMap(Map<String, Object> saslConfigMap) {
        StringBuilder config = new StringBuilder();
        if (saslConfigMap != null) {
            saslConfigMap.forEach((key, value) -> config.append("\n    ").append(key).append(": ").append(value));
        }
        return config.toString();
    }
}
