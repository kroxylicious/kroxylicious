/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import com.azure.core.credential.BasicAuthenticationCredential;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.core.http.HttpClient;
import com.azure.core.util.polling.SyncPoller;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.models.CreateKeyOptions;
import com.azure.security.keyvault.keys.models.DeletedKey;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.azure.security.keyvault.keys.models.KeyType;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.github.nagyesta.lowkeyvault.http.ApacheHttpClient;
import com.github.nagyesta.lowkeyvault.http.AuthorityOverrideFunction;
import com.github.nagyesta.lowkeyvault.http.management.LowkeyVaultException;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;
import com.sun.net.httpserver.HttpServer;

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.AzureKeyVaultKmsService;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder.lowkeyVault;

public class AzureKeyVaultKmsTestKmsFacade implements TestKmsFacade<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

    public static final Tls INSECURE_TLS = new Tls(null, new InsecureTls(true), null, null);
    @Nullable
    private LowkeyVaultContainer kms;
    @Nullable
    private HttpServer entraMock;

    protected AzureKeyVaultKmsTestKmsFacade() {
    }

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    public void startKms() {
        this.kms = startVault();
        try {
            this.entraMock = HttpServer.create();
            entraMock.createContext("/", exchange -> {
                exchange.getResponseHeaders().add("Content-type", "application/json");
                String responseBody = """
                        {
                            "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik5HVEZ2ZEstZnl0aEV1Q",
                            "token_type": "Bearer",
                            "expires_in": 3599,
                            "scope": "https%3A%2F%2Fgraph.microsoft.com%2Fmail.read",
                            "refresh_token": "AwABAAAAvPM1KaPlrEqdFSBzjqfTGAMxZGUTdM0t4B4",
                            "id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJhdWQiOiIyZDRkMTFhMi1mODE0LTQ2YTctOD"
                        }
                        """;
                exchange.getRequestBody().readAllBytes();
                exchange.getRequestBody().close();
                byte[] responseBodyBytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, responseBodyBytes.length);
                exchange.getResponseBody().write(responseBodyBytes);
                exchange.getResponseBody().close();
            });
            entraMock.bind(new InetSocketAddress(Inet4Address.getLocalHost(), 0), 1000);
            entraMock.start();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void stopKms() {
        if (kms != null) {
            kms.stop();
        }
    }

    public LowkeyVaultContainer startVault() {
        final DockerImageName imageName = DockerImageName.parse("nagyesta/lowkey-vault:4.0.0");
        final LowkeyVaultContainer lowkeyVaultContainer = lowkeyVault(imageName)
                .vaultNames(Set.of("default"))
                .build()
                .withImagePullPolicy(PullPolicy.defaultPolicy());
        lowkeyVaultContainer.start();
        return lowkeyVaultContainer;
    }

    @Override
    public final void start() {
        startKms();
    }

    @Override
    public AzureKeyVaultConfig getKmsServiceConfig() {
        if (kms == null) {
            throw new IllegalStateException("kms is not initialized");
        }
        if (entraMock == null) {
            throw new IllegalStateException("entraMock is not initialized");
        }
        InetSocketAddress address = entraMock.getAddress();
        String entraBaseUrl = "http://" + address.getHostName() + ":" + address.getPort();
        return new AzureKeyVaultConfig(
                new EntraIdentityConfig(entraBaseUrl, "identity", new InlinePassword("abc"), new InlinePassword("def"), null, INSECURE_TLS),
                kms.getDefaultVaultBaseUrl(), INSECURE_TLS);
    }

    @Override
    public final Class<AzureKeyVaultKmsService> getKmsServiceClass() {
        return AzureKeyVaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopKms();
    }

    @Override
    public final TestKekManager getTestKekManager() {
        if (kms == null) {
            throw new IllegalStateException("kms is not initialized");
        }
        return new AzureKmsTestKekManager(kms);
    }

    private static class AzureKmsTestKekManager implements TestKekManager {
        KeyClient keyClient;

        private AzureKmsTestKekManager(LowkeyVaultContainer kms) {
            keyClient = new KeyClientBuilder().credential(new BasicAuthenticationCredential("abc", "def"))
                    .httpClient(createHttpClient(kms.getEndpointAuthority(), kms.getDefaultVaultAuthority())).vaultUrl(kms.getDefaultVaultBaseUrl())
                    .disableChallengeResourceVerification().buildClient();
        }

        @Override
        public void generateKek(String alias) {
            String normalizedAlias = normalize(alias);
            // createKey succeeds imdempotently, not sure if azure or lowkey behaviour
            try {
                read(alias);
                throw new IllegalStateException("key '" + normalizedAlias + "' already exists");
            }
            catch (UnknownAliasException e) {
                keyClient.createKey(
                        new CreateKeyOptions(normalizedAlias, KeyType.RSA).setKeyOperations(KeyOperation.ENCRYPT, KeyOperation.DECRYPT, KeyOperation.WRAP_KEY,
                                KeyOperation.UNWRAP_KEY));
            }
        }

        @Override
        public KeyVaultKey read(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                return keyClient.getKey(normalizedAlias);
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        @Override
        public void deleteKek(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                SyncPoller<DeletedKey, Void> poller = keyClient.beginDeleteKey(normalizedAlias);
                poller.waitForCompletion(Duration.ofSeconds(10));
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            String normalizedAlias = normalize(alias);
            try {
                keyClient.rotateKey(normalizedAlias);
            }
            catch (ResourceNotFoundException e) {
                throw new UnknownAliasException(normalizedAlias);
            }
        }

        // the existing tests expect topic names containing _ can be used as KEK
        private static String normalize(String alias) {
            return alias.replaceAll("_", "-");
        }

        private static HttpClient createHttpClient(String clientAuthority, String containerAuthority) {
            try {
                SSLContextBuilder builder = new SSLContextBuilder();
                builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
                return new ApacheHttpClient(new AuthorityOverrideFunction(clientAuthority, containerAuthority), new TrustSelfSignedStrategy(),
                        new DefaultHostnameVerifier());
            }
            catch (Exception e) {
                throw new LowkeyVaultException("Failed to create HTTP client.", e);
            }
        }
    }
}
