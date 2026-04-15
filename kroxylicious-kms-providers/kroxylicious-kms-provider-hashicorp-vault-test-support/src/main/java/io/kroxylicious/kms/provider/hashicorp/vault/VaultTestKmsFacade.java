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
    private static final String IMAGE = "hashicorp/vault:2.0.0@sha256:e40c741ed95bb271425e3e6ca6c222d620cf8682f6f7a1b1e7c9d49d0aba484b";
    private static final DockerImageName HASHICORP_VAULT = DockerImageName.parse(IMAGE)
            .asCompatibleSubstituteFor(DockerImageName.parse(IMAGE.substring(0, IMAGE.indexOf("@"))));

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
