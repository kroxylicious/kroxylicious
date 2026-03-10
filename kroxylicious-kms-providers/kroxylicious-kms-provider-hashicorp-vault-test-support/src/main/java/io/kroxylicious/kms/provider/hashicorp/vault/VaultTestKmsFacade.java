/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import edu.umd.cs.findbugs.annotations.Nullable;

public class VaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private static final String IMAGE = "hashicorp/vault:1.21.4@sha256:4e33b126a59c0c333b76fb4e894722462659a6bec7c48c9ee8cea56fccfd2569";
    private static final DockerImageName HASHICORP_VAULT = DockerImageName.parse(IMAGE).asCompatibleSubstituteFor(DockerImageName.parse(IMAGE.substring(0, IMAGE.indexOf("@"))));

    @SuppressWarnings("rawtypes")
    private @Nullable VaultContainer vaultContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void startVault() {
        vaultContainer = new VaultContainer<>(HASHICORP_VAULT).withVaultToken(VAULT_ROOT_TOKEN);
        vaultContainer.start();
    }

    @Override
    public void stopVault() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Override
    protected URI getVaultUrl() {
        return URI.create(vaultContainer.getHttpHostAddress());
    }
}
