/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.TlsSubjectBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.tlssubjectbuilder.ConfigTemplate;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import static java.time.Clock.systemUTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class VirtualKafkaClusterSubjectBuilderRefsTest {

    private static final String NAMESPACE = "my-namespace";

    private final VirtualKafkaClusterReconciler reconciler = new VirtualKafkaClusterReconciler(
            systemUTC(), DependencyResolver.create(), mock(SharedInformerManager.class),
            SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

    // @formatter:off
    @Test
    void noTlsSubjectBuilder_returnsEmpty() {
        // Given
        var cluster = clusterWithNoTlsSubjectBuilder();
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster)).isEmpty();
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster)).isEmpty();
    }

    @Test
    void noConfigTemplate_returnsEmpty() {
        // Given
        var tsb = new TlsSubjectBuilder();
        tsb.setType("com.example.MyBuilder");
        var cluster = clusterWith(tsb);
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster)).isEmpty();
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster)).isEmpty();
    }

    @Test
    void secretReferenceOnly_returnsSecretRefOnly() {
        // Given
        var cluster = clusterWithTemplate("key", "${secret:my-secret:val}", null, null);
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster))
                .extracting(LocalRef::getName).containsExactly("my-secret");
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster)).isEmpty();
    }

    @Test
    void configMapReferenceOnly_returnsConfigMapRefOnly() {
        // Given
        var cluster = clusterWithTemplate(null, null, "key", "${configmap:my-configmap:val}");
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster)).isEmpty();
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster))
                .extracting(LocalRef::getName).containsExactly("my-configmap");
    }

    @Test
    void bothReferences_returnsBothRefs() {
        // Given
        var cluster = clusterWithTemplate("sk", "${secret:my-secret:val}", "ck", "${configmap:my-configmap:val}");
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster))
                .extracting(LocalRef::getName).containsExactly("my-secret");
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster))
                .extracting(LocalRef::getName).containsExactly("my-configmap");
    }

    @Test
    void configTemplateWithNoReferences_returnsEmpty() {
        // Given
        var ct = new ConfigTemplate();
        ct.setAdditionalProperty("searchBase", "ou=users,dc=example,dc=com");
        var tsb = new TlsSubjectBuilder();
        tsb.setType("com.example.MyBuilder");
        tsb.setConfigTemplate(ct);
        var cluster = clusterWith(tsb);
        // When / Then
        assertThat(reconciler.subjectBuilderSecretRefs(cluster)).isEmpty();
        assertThat(reconciler.subjectBuilderConfigMapRefs(cluster)).isEmpty();
    }
    // @formatter:on

    private VirtualKafkaCluster clusterWithNoTlsSubjectBuilder() {
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata().withName("c").withUid(UUID.randomUUID().toString()).withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                .withNewProxyRef().withName("p").endProxyRef()
                .addNewIngress().withNewIngressRef().withName("i").endIngressRef().endIngress()
                .withNewTargetKafkaServiceRef().withName("k").endTargetKafkaServiceRef()
                .endSpec().build();
    }

    private VirtualKafkaCluster clusterWith(TlsSubjectBuilder tsb) {
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata().withName("c").withUid(UUID.randomUUID().toString()).withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                .withNewProxyRef().withName("p").endProxyRef()
                .addNewIngress().withNewIngressRef().withName("i").endIngressRef().endIngress()
                .withNewTargetKafkaServiceRef().withName("k").endTargetKafkaServiceRef()
                .withTlsSubjectBuilder(tsb)
                .endSpec().build();
    }

    private VirtualKafkaCluster clusterWithTemplate(String secretKey, String secretVal, String cmKey, String cmVal) {
        var ct = new ConfigTemplate();
        if (secretKey != null) {
            ct.setAdditionalProperty(secretKey, secretVal);
        }
        if (cmKey != null) {
            ct.setAdditionalProperty(cmKey, cmVal);
        }
        var tsb = new TlsSubjectBuilder();
        tsb.setType("com.example.MyBuilder");
        tsb.setConfigTemplate(ct);
        return clusterWith(tsb);
    }
}
