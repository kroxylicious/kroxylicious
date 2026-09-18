/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.rewrite.v0_25;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.yaml.Assertions.yaml;

@SuppressWarnings("java:S2699") // rewriteRun contains assertions
class UseClusterDefinitionsTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new UseClusterDefinitions(null));
    }

    @Test
    void shouldMigrateVirtualClusterWithNestedTls() {
        rewriteRun(
                yaml(
                        // Before (Input configuration)
                        """
                                management:
                                  endpoints:
                                    prometheus: {}
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                      tls:
                                        trust:
                                          storeFile: /x/trust.p12
                                      bootstrapServerSelection:
                                        strategy: "round-robin"
                                    gateways:
                                      - name: mygateway
                                        portIdentifiesNode:
                                          bootstrapAddress: localhost:9192
                                """,
                        // After (Expected transformed configuration)
                        """
                                management:
                                  endpoints:
                                    prometheus: {}
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092
                                    tls:
                                      trust:
                                        storeFile: /x/trust.p12
                                    bootstrapServerSelection:
                                      strategy: "round-robin"
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                    gateways:
                                      - name: mygateway
                                        portIdentifiesNode:
                                          bootstrapAddress: localhost:9192
                                """));
    }

    @Test
    void shouldMigrateMinimalTargetCluster() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldMigrateMultipleVirtualClustersInSourceOrder() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: bar
                                    targetCluster:
                                      bootstrapServers: first.example:9092
                                  - name: foo
                                    targetCluster:
                                      bootstrapServers: second.example:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: bar-target
                                    bootstrapServers: first.example:9092
                                  - name: foo-target
                                    bootstrapServers: second.example:9092
                                virtualClusters:
                                  - name: bar
                                    target:
                                      cluster: bar-target
                                  - name: foo
                                    target:
                                      cluster: foo-target
                                """));
    }

    @Test
    void shouldMigrateBootstrapServerSelection() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: broker-0.example:9092,broker-1.example:9092
                                      bootstrapServerSelection:
                                        strategy: "round-robin"
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: broker-0.example:9092,broker-1.example:9092
                                    bootstrapServerSelection:
                                      strategy: "round-robin"
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldPreserveEmptyTlsMapping() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                      tls: {}
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092
                                    tls: {}
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldLeaveVirtualClustersAlreadyUsingTargetUntouched() {
        rewriteRun(
                yaml(
                        """
                                clusterDefinitions:
                                  - name: new-cluster
                                    bootstrapServers: new.example:1234
                                virtualClusters:
                                  - name: old-style
                                    targetCluster:
                                      bootstrapServers: old.example:1234
                                  - name: new-style
                                    target:
                                      cluster: new-cluster
                                """,
                        """
                                clusterDefinitions:
                                  - name: new-cluster
                                    bootstrapServers: new.example:1234
                                  - name: old-style-target
                                    bootstrapServers: old.example:1234
                                virtualClusters:
                                  - name: old-style
                                    target:
                                      cluster: old-style-target
                                  - name: new-style
                                    target:
                                      cluster: new-cluster
                                """));
    }

    @Test
    void shouldAvoidCollisionWithExistingClusterDefinitionName() {
        rewriteRun(
                yaml(
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: other.example:1234
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: other.example:1234
                                  - name: demo-target-2
                                    bootstrapServers: localhost:9092
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target-2
                                """));
    }

    @Test
    void shouldPreserveFourSpaceIndentStyle() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                    -   name: demo
                                        targetCluster:
                                            bootstrapServers: localhost:9092
                                            tls:
                                                trust:
                                                    storeFile: /x/trust.p12
                                """,
                        """
                                clusterDefinitions:
                                    - name: demo-target
                                      bootstrapServers: localhost:9092
                                      tls:
                                          trust:
                                              storeFile: /x/trust.p12
                                virtualClusters:
                                    -   name: demo
                                        target:
                                            cluster: demo-target
                                """));
    }

    @Test
    void shouldPreserveCommentsWithinTargetCluster() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      # the brokers to proxy
                                      bootstrapServers: localhost:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    # the brokers to proxy
                                    bootstrapServers: localhost:9092
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldPreserveInlineCommentWithinTargetCluster() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092 # the brokers to proxy
                                      tls: {}
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092 # the brokers to proxy
                                    tls: {}
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    // a text block cannot express \r\n: the compiler normalises its line terminators to \n, so concatenation is the
    // only way to write a CRLF source
    @SuppressWarnings("StringConcatToTextBlock")
    void shouldPreserveWindowsLineEndings() {
        rewriteRun(
                yaml(
                        "virtualClusters:\r\n"
                                + "  - name: demo\r\n"
                                + "    targetCluster:\r\n"
                                + "      bootstrapServers: localhost:9092\r\n"
                                + "      tls:\r\n"
                                + "        trust:\r\n"
                                + "          storeFile: /x/trust.p12\r\n",
                        "clusterDefinitions:\r\n"
                                + "  - name: demo-target\r\n"
                                + "    bootstrapServers: localhost:9092\r\n"
                                + "    tls:\r\n"
                                + "      trust:\r\n"
                                + "        storeFile: /x/trust.p12\r\n"
                                + "virtualClusters:\r\n"
                                + "  - name: demo\r\n"
                                + "    target:\r\n"
                                + "      cluster: demo-target\r\n"));
    }

    @Test
    void shouldPreserveQuotedScalarStyle() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: "localhost:9092"
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: "localhost:9092"
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldPreserveBlockScalarBootstrapServers() {
        // both TargetCluster and ClusterDefinition strip whitespace from bootstrapServers, so a long list may be
        // written as a block scalar. Its body must be re-indented along with the rest of the block that moves.
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: |-
                                        broker-0.example:9092,
                                        broker-1.example:9092,
                                        broker-2.example:9092
                                      tls:
                                        trust:
                                          storeFile: /x/trust.p12
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: |-
                                      broker-0.example:9092,
                                      broker-1.example:9092,
                                      broker-2.example:9092
                                    tls:
                                      trust:
                                        storeFile: /x/trust.p12
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldMigrateEachDocumentOfMultiDocumentFile() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: first
                                    targetCluster:
                                      bootstrapServers: first.example:9092
                                ---
                                virtualClusters:
                                  - name: second
                                    targetCluster:
                                      bootstrapServers: second.example:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: first-target
                                    bootstrapServers: first.example:9092
                                virtualClusters:
                                  - name: first
                                    target:
                                      cluster: first-target
                                ---
                                clusterDefinitions:
                                  - name: second-target
                                    bootstrapServers: second.example:9092
                                virtualClusters:
                                  - name: second
                                    target:
                                      cluster: second-target
                                """));
    }

    @Test
    void shouldOnlyMigrateFilesMatchingFilePattern() {
        rewriteRun(
                spec -> spec.recipe(new UseClusterDefinitions("**/proxy-config.yaml")),
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                """,
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """,
                        source -> source.path("proxy-config.yaml")),
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                """,
                        source -> source.path("something-else.yaml")));
    }

    @Test
    void shouldBeIdempotent() {
        // A single argument to yaml implies it doesn't make changes.
        rewriteRun(
                yaml(
                        """
                                clusterDefinitions:
                                  - name: demo-target
                                    bootstrapServers: localhost:9092
                                virtualClusters:
                                  - name: demo
                                    target:
                                      cluster: demo-target
                                """));
    }

    @Test
    void shouldNotChangeConfigurationWithoutVirtualClusters() {
        rewriteRun(
                yaml(
                        """
                                management:
                                  endpoints:
                                    prometheus: {}
                                """));
    }

    @Test
    void shouldNotChangeKubernetesManifest() {
        rewriteRun(
                yaml(
                        """
                                apiVersion: kroxylicious.io/v1alpha1
                                kind: KafkaProxy
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                """));
    }

    @Test
    void shouldNotChangeVirtualClustersNestedBelowTheDocumentRoot() {
        rewriteRun(
                yaml(
                        """
                                spec:
                                  virtualClusters:
                                    - name: demo
                                      targetCluster:
                                        bootstrapServers: localhost:9092
                                """));
    }

    @Test
    void shouldNotChangeVirtualClusterDeclaringBothTargetAndTargetCluster() {
        rewriteRun(
                yaml(
                        """
                                clusterDefinitions:
                                  - name: demo-cluster
                                    bootstrapServers: other.example:1234
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                    target:
                                      cluster: demo-cluster
                                """));
    }

    @Test
    void shouldNotChangeTargetClusterWithoutBootstrapServers() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      tls: {}
                                """));
    }

    @Test
    void shouldNotChangeTargetClusterUsingAnchors() {
        rewriteRun(
                yaml(
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: localhost:9092
                                      tls: &commonTls
                                        trust:
                                          storeFile: /x/trust.p12
                                """));
    }
}
