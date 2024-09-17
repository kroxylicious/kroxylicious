/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacade;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultTestKmsFacade;
import io.kroxylicious.systemtests.installation.kms.vault.Vault;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.utils.VersionComparator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.VAULT_ROOT_TOKEN;

/**
 * KMS Facade for Vault running inside Kube.
 */
public class KubeVaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private final Vault vault;

    /**
     * Instantiates a new Kube vault test kms facade.
     *
     */
    public KubeVaultTestKmsFacade() {
        this.vault = new Vault(VAULT_ROOT_TOKEN);
    }

    @Override
    public void startVault() {
        vault.deploy();
        String installedVersion = getVaultVersion();
        String expectedVersion = VaultTestKmsFacade.HASHICORP_VAULT.getVersionPart();
        if (!isCorrectVersionInstalled(installedVersion, expectedVersion)) {
            throw new KubeClusterException("Vault version installed " + installedVersion + " does not match with the expected: '"
                    + expectedVersion + "'");
        }
    }

    private boolean isCorrectVersionInstalled(String installedVersion, String expectedVersion) {
        VersionComparator comparator = new VersionComparator(installedVersion);
        return comparator.compareTo(expectedVersion) == 0;
    }

    @Override
    public void stopVault() {
        try {
            vault.delete();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete Vault", e);
        }
    }

    @NonNull
    @Override
    protected URI getVaultUrl() {
        return vault.getVaultUrl();
    }

    /**
     * Gets vault version.
     *
     * @return the vault version
     */
    public String getVaultVersion() {
        return vault.getVersionInstalled();
    }
}
