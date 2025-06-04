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

import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.tls.CertificateGenerator;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVault implements Closeable {
    public static final int PLAIN_PORT = 8200;
    private static final String VAULT_TOKEN = "token";

    private static final DockerImageName HASHICORP_VAULT = DockerImageName.parse("hashicorp/vault:1.19.5");

    private final VaultContainer<?> vault;
    private final URI endpoint;

    private static final int TLS_PORT = 8202;

    @SuppressWarnings("resource")
    private TestVault(CertificateGenerator.Keys serverKeys, CertificateGenerator.Keys clientKeys) {

        if (clientKeys != null && serverKeys == null) {
            throw new IllegalArgumentException("server TLS key/cert must be supplied if we want to configure client TLS auth");
        }
        VaultContainer<?> vault = new VaultContainer<>(HASHICORP_VAULT)
                .withVaultToken(VAULT_TOKEN)
                .withEnv("VAULT_FORMAT", "json")
                .withInitCommand("secrets enable transit");
        if (serverKeys != null) {
            withTls(vault, serverKeys, clientKeys);
        }
        vault.start();
        this.vault = vault;
        endpoint = URI.create(serverKeys == null ? vault.getHttpHostAddress() : String.format("https://%s:%s", vault.getHost(), vault.getMappedPort(TLS_PORT)))
                .resolve("v1/transit");
    }

    private static void withTls(VaultContainer<?> vault, CertificateGenerator.Keys serverKeys, CertificateGenerator.Keys clientKeys) {
        boolean isMutualTls = clientKeys != null;
        String configFile = isMutualTls ? "VaultMutualTls.hcl" : "Vault.hcl";
        int healthPort = isMutualTls ? PLAIN_PORT : TLS_PORT;
        vault.withExposedPorts(TLS_PORT, healthPort);
        vault.withCopyFileToContainer(MountableFile.forClasspathResource(configFile), "/vault/config/Vault.hcl")
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeys.selfSignedCertificatePem()), "/vault/config/cert.pem")
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeys.privateKeyPem()), "/vault/config/key.pem");
        if (isMutualTls) {
            vault.withCopyFileToContainer(MountableFile.forHostPath(clientKeys.selfSignedCertificatePem()), "/vault/config/client-cert.pem");
        }
        HttpWaitStrategy wait = Wait.forHttp("/v1/sys/health").forPort(healthPort).forStatusCode(200);
        if (!isMutualTls) {
            // when using server TLS only, we only expose the tls port.
            wait.usingTls().allowInsecure().forStatusCode(200);
        }
        vault.setWaitStrategy(wait);
    }

    public static TestVault startWithTls(CertificateGenerator.Keys keys) {
        return new TestVault(keys, null);
    }

    public static TestVault startWithClientAuthTls(CertificateGenerator.Keys serverKeys, CertificateGenerator.Keys clientKeys) {
        return new TestVault(serverKeys, clientKeys);
    }

    public static TestVault start() {
        return new TestVault(null, null);
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

    VaultResponse.ReadKeyData createKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "write", "-f", "transit/keys/%s".formatted(keyId));
    }

}
