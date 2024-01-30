/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVault implements Closeable {
    private static final String VAULT_TOKEN = "token";

    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";

    private final VaultContainer<?> vault;
    private final URI endpoint;

    private static final int TLS_PORT = 8202;

    private TestVault(CertificateGenerator.Keys keys) {
        VaultContainer<?> vault = new VaultContainer<>(HASHICORP_VAULT)
                .withVaultToken(VAULT_TOKEN)
                .withEnv("VAULT_FORMAT", "json")
                .withInitCommand("secrets enable transit");
        if (keys != null) {
            // in vault server -dev mode the 8200 port cannot be reconfigured to TLS. So we configure a second listener on a
            // different port. We expose only the tls port to avoid any chance of a misconfigured client pointing at the plain listener.
            vault = vault.withExposedPorts(TLS_PORT)
                    .withCopyFileToContainer(MountableFile.forClasspathResource("Vault.hcl"), "/vault/config/Vault.hcl")
                    .withCopyFileToContainer(MountableFile.forHostPath(keys.serverCertPem()), "/vault/config/cert.pem")
                    .withCopyFileToContainer(MountableFile.forHostPath(keys.serverPrivateKeyPem()), "/vault/config/key.pem");
            // we need testcontainers to allow insecure connections to the health as the only exposed port is now using TLS
            vault.setWaitStrategy(Wait.forHttps("/v1/sys/health").allowInsecure().forStatusCode(200));
        }
        vault.start();
        this.vault = vault;
        endpoint = URI.create(keys == null ? vault.getHttpHostAddress() : String.format("https://%s:%s", vault.getHost(), vault.getMappedPort(TLS_PORT)));
    }

    public static TestVault startWithTls(CertificateGenerator.Keys keys) {
        return new TestVault(keys);
    }

    public static TestVault start() {
        return new TestVault(null);
    }

    public URI getEndpoint() {
        return endpoint;
    }

    @Override
    public void close() {
        vault.close();
    }

    <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... args) {
        try {
            var execResult = vault.execInContainer(args);
            int exitCode = execResult.getExitCode();
            assertThat(exitCode).isZero();
            var response = new ObjectMapper().readValue(execResult.getStdout(), valueTypeRef);
            return response.data();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to run vault command: %s".formatted(Arrays.stream(args).toList()), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public String rootToken() {
        return VAULT_TOKEN;
    }
}
