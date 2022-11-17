/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster.extension;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.test.kafkacluster.ContainerBasedKafkaCluster;
import io.kroxylicious.test.kafkacluster.KafkaCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(KafkaClusterExtension.class)
public class KafkaClusterExtensionTest {

    @BrokerCluster(numBrokers = 1)
    static KafkaCluster staticCluster;

    @BrokerCluster(numBrokers = 1)
    KafkaCluster instanceCluster;

    @Test
    public void testKafkaClusterStaticField()
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(staticCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(staticCluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(ContainerBasedKafkaCluster.class, staticCluster);
    }

    @Test
    public void testKafkaClusterInstanceField()
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(instanceCluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertEquals(instanceCluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(ContainerBasedKafkaCluster.class, instanceCluster);
    }

    @Test
    public void testKafkaClusterParameter(
                                          @BrokerCluster(numBrokers = 2) KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(cluster.getKafkaClientConfiguration());
        assertEquals(2, dc.nodes().get().size());
        assertEquals(cluster.getClusterId(), dc.clusterId().get());
        var cbc = assertInstanceOf(ContainerBasedKafkaCluster.class, cluster);
    }

    @Test
    public void testTwoKafkaClusterParameter(
                                             @BrokerCluster(numBrokers = 1) KafkaCluster cluster1,
                                             @BrokerCluster(numBrokers = 2) KafkaCluster cluster2)
            throws ExecutionException, InterruptedException {
        assertNotEquals(cluster1.getClusterId(), cluster2.getClusterId());
        var dc1 = assertCluster(cluster1.getKafkaClientConfiguration());
        assertEquals(1, dc1.nodes().get().size());
        assertEquals(cluster1.getClusterId(), dc1.clusterId().get());
        var dc2 = assertCluster(cluster2.getKafkaClientConfiguration());
        assertEquals(2, dc2.nodes().get().size());
        assertEquals(cluster2.getClusterId(), dc2.clusterId().get());
    }

    @Test
    public void testZkBasedKafkaCluster(
                                        @BrokerCluster @ZooKeeperCluster KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(cluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
        assertNull(cluster.getClusterId(),
                "KafkaCluster.getClusterId() should be null for ZK-based clusters");
    }

    @Test
    public void testKraftBasedKafkaCluster(
                                           @BrokerCluster @KRaftCluster KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(cluster.getKafkaClientConfiguration());
        assertEquals(1, dc.nodes().get().size());
    }

    @Test
    public void testSaslPlainCluster(
                                     @BrokerCluster @SaslPlainAuth({
                                             @SaslPlainAuth.UserPassword(user = "alice", password = "foo"),
                                             @SaslPlainAuth.UserPassword(user = "bob", password = "bar")
                                     }) KafkaCluster cluster)
            throws ExecutionException, InterruptedException {
        var dc = assertCluster(cluster.getKafkaClientConfiguration("alice", "foo"));
        assertEquals(1, dc.nodes().get().size());
        assertEquals(cluster.getClusterId(), dc.clusterId().get());

        dc = assertCluster(cluster.getKafkaClientConfiguration("bob", "bar"));
        assertEquals(cluster.getClusterId(), dc.clusterId().get());

        var ee = assertThrows(ExecutionException.class, () -> assertCluster(cluster.getKafkaClientConfiguration("bob", "baz")),
                "Expect bad password to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());

        ee = assertThrows(ExecutionException.class, () -> assertCluster(cluster.getKafkaClientConfiguration("eve", "quux")),
                "Expect unknown user to throw");
        assertInstanceOf(SaslAuthenticationException.class, ee.getCause());
    }

    private static DescribeClusterResult assertCluster(Map<String, Object> adminConfig) throws InterruptedException, ExecutionException {
        try (var admin = Admin.create(adminConfig)) {
            DescribeClusterResult describeClusterResult = admin.describeCluster();
            describeClusterResult.controller().get();
            return describeClusterResult;
        }
    }

}
