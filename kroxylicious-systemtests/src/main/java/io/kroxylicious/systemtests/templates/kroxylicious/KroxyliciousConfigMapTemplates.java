/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

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
        aclRules.sort(Collections.reverseOrder()); // deny should be always first, then allow sentences
        aclRules.forEach(rule -> aclRule.append("\n").append(rule));
        aclRule.append("\n").append("otherwise deny;");
        return aclRule.toString();
    }

    /**
     * Gets config map for sasl config.
     *
     * @param namespace the namespace
     * @param name the name
     * @param securityProtocol the security protocol
     * @param saslMechanism the sasl mechanism
     * @return  the config map for additional config
     */
    public static ConfigMapBuilder getConfigMapForSaslConfig(String namespace, String name, String securityProtocol, String saslMechanism, String username,
                                                             String usernamePassword) {
        Properties adminConfig = new Properties();
        adminConfig.put("ADDITIONAL_CONFIG", CommonClientConfigs.SECURITY_PROTOCOL_CONFIG + "=" + securityProtocol
                + "\n" + SaslConfigs.SASL_MECHANISM + "=" + saslMechanism
                + "\n" + SaslConfigs.SASL_JAAS_CONFIG + "=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username +
                "\" password=\"" + usernamePassword + "\";");

        String properties;
        try (StringWriter writer = new StringWriter()) {
            adminConfig.store(writer, "admin config");
            properties = writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of(Constants.CONFIG_PROP_FILE_NAME, properties));
        // @formatter:on
    }
}
