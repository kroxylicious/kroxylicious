/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import org.junit.jupiter.api.Test;

import io.kroxylicious.systemtests.resources.kms.aws.KubeAwsKmsCloudTestKmsFacade;
import io.kroxylicious.systemtests.resources.kms.aws.KubeLocalStackTestKmsFacade;
import io.kroxylicious.systemtests.resources.kms.azure.KubeLowKeyVaultKmsTestKmsFacade;
import io.kroxylicious.systemtests.resources.kms.vault.KubeVaultKubernetesAuthTestKmsFacade;
import io.kroxylicious.systemtests.resources.kms.vault.KubeVaultTestKmsFacade;
import io.kroxylicious.testing.kms.fortanix.FortanixDsmKmsTestKmsFacade;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the facade selection used by {@code RecordEncryptionST} and {@code AlternativeAuthRecordEncryptionST}.
 * This exercises the real {@link java.util.ServiceLoader} discovery and construction of every in-cluster facade,
 * so it guards against the two STs accidentally selecting the same, or the wrong, facades.
 */
class KmsFacadesTest {

    @Test
    void recordEncryptionSelectionYieldsStaticCredentialFacades() {
        // RecordEncryptionST selects every in-cluster facade that is not ServiceAccount-auth.
        assertThat(KmsFacades.inCluster(f -> !(f instanceof ServiceAccountAuthTestKmsFacade)))
                .extracting(Object::getClass)
                .containsExactlyInAnyOrder(
                        KubeAwsKmsCloudTestKmsFacade.class,
                        KubeLocalStackTestKmsFacade.class,
                        KubeVaultTestKmsFacade.class,
                        FortanixDsmKmsTestKmsFacade.class,
                        KubeLowKeyVaultKmsTestKmsFacade.class);
    }

    @Test
    void alternativeAuthSelectionYieldsOnlyServiceAccountAuthFacade() {
        // AlternativeAuthRecordEncryptionST selects only the ServiceAccount-auth facade(s).
        assertThat(KmsFacades.inCluster(ServiceAccountAuthTestKmsFacade.class::isInstance))
                .extracting(Object::getClass)
                .containsExactly(KubeVaultKubernetesAuthTestKmsFacade.class);
    }
}
