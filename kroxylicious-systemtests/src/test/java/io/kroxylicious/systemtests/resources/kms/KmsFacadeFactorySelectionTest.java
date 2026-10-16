/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import org.junit.jupiter.api.Test;

import io.kroxylicious.systemtests.resources.kms.aws.KubeAwsKmsCloudTestKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.aws.KubeLocalStackTestKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.azure.KubeLowKeyVaultTestKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.fortanix.KubeFortanixTestKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.vault.KubeVaultKubernetesAuthTestKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.vault.KubeVaultTestKmsFacadeFactory;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the factory selection used by {@code RecordEncryptionST} and {@code ServiceAccountAuthRecordEncryptionST}.
 * This exercises the real {@link java.util.ServiceLoader} discovery of every registered factory, so it guards
 * against the two STs accidentally selecting the same, or the wrong, facades. It inspects factory types only,
 * so no facade is built and no cluster is required.
 */
class KmsFacadeFactorySelectionTest {

    @Test
    void staticCredentialFactoriesAreTheFiveInClusterKubeFactories() {
        // RecordEncryptionST selects every StaticCredentialAuthKmsFacadeFactory.
        assertThat(TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(StaticCredentialAuthKmsFacadeFactory.class::isInstance))
                .extracting(Object::getClass)
                .containsExactlyInAnyOrder(
                        KubeVaultTestKmsFacadeFactory.class,
                        KubeLocalStackTestKmsFacadeFactory.class,
                        KubeAwsKmsCloudTestKmsFacadeFactory.class,
                        KubeFortanixTestKmsFacadeFactory.class,
                        KubeLowKeyVaultTestKmsFacadeFactory.class);
    }

    @Test
    void serviceAccountAuthFactoriesAreOnlyTheVaultKubernetesAuthFactory() {
        // ServiceAccountAuthRecordEncryptionST selects only the ServiceAccount-auth factories.
        assertThat(TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(ServiceAccountAuthKmsFacadeFactory.class::isInstance))
                .extracting(Object::getClass)
                .containsExactly(KubeVaultKubernetesAuthTestKmsFacadeFactory.class);
    }
}
