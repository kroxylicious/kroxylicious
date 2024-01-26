/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.ssl.SSLContextBuilder;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for HashiCorp Vault.
 * <br/>
 * <h4>TODO</h4>
 * <ul>
 *    <li>test condition such as wrong kek type, expired kek</li>
 * </ul>
 */
class VaultKmsIT {

    private static final String VAULT_TOKEN = "token";

    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";
    @SuppressWarnings("rawtypes")
    private VaultContainer vaultContainer;
    private static final String nonTlsService = "nonTls";
    private static final String tlsService = "tls";
    private Map<String, VaultKms> vaultKmsMap = new HashMap<>();
    private URI vaultTlsUri;
    private FilterFactoryContext mockTlsProvidingContext;

    static List<String> vaultKms() {
        return List.of(nonTlsService, tlsService);
    }

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();

        vaultContainer = new VaultContainer<>(HASHICORP_VAULT)
                .withVaultToken(VAULT_TOKEN)
                .withEnv("VAULT_FORMAT", "json")
                // in vault server -dev mode the 8200 port cannot be reconfigured to TLS and is also useful so that the
                // internal vault commands we make while execed into the container can communicate with vault over plain
                // without having to trust the TLS cert within the container. Plus we can test TLS and non-TLS against the
                // same container.
                .withExposedPorts(8200, 8202)
                .withCopyFileToContainer(MountableFile.forClasspathResource("Vault.hcl"), "/vault/config/Vault.hcl")
                .withCopyFileToContainer(MountableFile.forClasspathResource("cert.pem", 0777), "/vault/config/cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("key.pem", 0777), "/vault/config/key.pem")
                .withInitCommand(
                        "secrets enable transit");
        vaultContainer.setWaitStrategy(Wait.forHttp("/v1/sys/health").forPort(8200).forStatusCode(200));
        vaultContainer.start();
        String nonTlsAddress = vaultContainer.getHttpHostAddress();
        URL resource = VaultKmsIT.class.getClassLoader().getResource("vault-truststore.pkcs12");

        var nonTlsConfig = new Config(URI.create(nonTlsAddress), VAULT_TOKEN, null);
        Tls arbitraryTlsConfiguration = new Tls(null, null);
        String tlsAddress = String.format("https://%s:%s", vaultContainer.getHost(), vaultContainer.getMappedPort(8202));
        vaultTlsUri = URI.create(tlsAddress);
        var tlsConfig = new Config(vaultTlsUri, VAULT_TOKEN, arbitraryTlsConfiguration);
        mockTlsProvidingContext = mockTlsProvidingContext(resource);
        vaultKmsMap.put(nonTlsService, new VaultKmsService().buildKms(nonTlsConfig, mockTlsProvidingContext));
        vaultKmsMap.put(tlsService, new VaultKmsService().buildKms(tlsConfig, mockTlsProvidingContext));
    }

    @NonNull
    private FilterFactoryContext mockTlsProvidingContext(URL resource) {
        SSLContext sslContext;
        try {
            sslContext = SSLContextBuilder.create().loadTrustMaterial(resource, "changeit".toCharArray()).build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new FilterFactoryContext() {
            @Override
            public ScheduledExecutorService eventLoop() {
                return null;
            }

            @Override
            public SSLContext clientSslContext(Tls tls) {
                return sslContext;
            }

            @Override
            public <P> @NonNull P pluginInstance(@NonNull Class<P> pluginClass, @NonNull String instanceName) {
                return null;
            }
        };
    }

    @AfterEach
    void afterEach() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Test
    void tlsConnectionFailsWithoutClientTrust() {
        // passing null tls means we do not attempt to obtain a custom SSLContext from FilterFactoryContext
        var tlsConfig = new Config(vaultTlsUri, VAULT_TOKEN, null);
        var keyName = "mykey";
        createKek(keyName);
        VaultKms service = new VaultKmsService().buildKms(tlsConfig, mockTlsProvidingContext);
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat().havingCause()
                .isInstanceOf(SSLHandshakeException.class)
                .withMessageContaining("unable to find valid certification path to requested target");
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void resolveKeyByName(String tlsService) {
        VaultKms service = vaultKmsMap.get(tlsService);
        var keyName = "mykey";
        createKek(keyName);
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(keyName);
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void resolveWithUnknownKey(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        var keyName = "unknown";
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void generatedEncryptedDekDecryptsBackToPlain(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        String key = "mykey";
        createKek(key);

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        var decryptedDekStage = service.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(pair.dek());
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void decryptDekAfterRotate(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        var key = "mykey";
        var data = createKek(key);
        var originalVersion = data.latestVersion();

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        var updated = rotateKek(data.name());
        var versionAfterRotate = updated.latestVersion();
        assertThat(versionAfterRotate).isGreaterThan(originalVersion);

        var decryptedDekStage = service.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(pair.dek());
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void generatedDekPairWithUnknownKey(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        var pairStage = service.generateDekPair("unknown");
        assertThat(pairStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void decryptEdekWithUnknownKey(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        var secretKeyStage = service.decryptEdek(new VaultEdek("unknown", new byte[]{ 1 }));
        assertThat(secretKeyStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @ParameterizedTest
    @MethodSource("vaultKms")
    void edekSerdeRoundTrip(String kms) {
        VaultKms service = vaultKmsMap.get(kms);
        var key = "mykey";
        createKek(key);

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();
        assertThat(pair).extracting(DekPair::edek).isNotNull();

        var edek = pair.edek();
        var serde = service.edekSerde();
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var output = serde.deserialize(buf);
        assertThat(output).isEqualTo(edek);
    }

    private ReadKeyData readKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "read", "transit/keys/%s".formatted(keyId));
    }

    private ReadKeyData createKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "write", "-f", "transit/keys/%s".formatted(keyId));
    }

    private ReadKeyData rotateKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "write", "-f", "transit/keys/%s/rotate".formatted(keyId));
    }

    private <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... args) {
        try {
            var execResult = vaultContainer.execInContainer(args);
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

}
