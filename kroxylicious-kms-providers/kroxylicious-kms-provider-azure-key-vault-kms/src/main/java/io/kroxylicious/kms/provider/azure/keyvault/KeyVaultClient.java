/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.azure.MalformedResponseBodyException;
import io.kroxylicious.kms.provider.azure.UnexpectedHttpStatusCodeException;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.auth.BearerToken;
import io.kroxylicious.kms.provider.azure.auth.BearerTokenService;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tls.TlsHttpClientConfigurator;

public class KeyVaultClient implements AutoCloseable {
    public static final String API_VERSION = "7.4";
    private final BearerTokenService service;
    private final HttpClient client;
    private final AzureKeyVaultConfig config;
    private final ObjectMapper mapper = new ObjectMapper();

    public KeyVaultClient(BearerTokenService service, AzureKeyVaultConfig config) {
        Objects.requireNonNull(service, "service cannot be null");
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
        this.service = service;
        HttpClient.Builder builder = HttpClient.newBuilder();
        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());
        tlsConfigurator.apply(builder);
        this.client = builder.version(HttpClient.Version.HTTP_1_1).followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(10L)).build();
    }

    public CompletionStage<byte[]> wrap(WrappingKey wrappingKey, byte[] bytes) {
        return wrapOrUnwrap(wrappingKey, bytes, "wrapkey");
    }

    public CompletionStage<byte[]> unwrap(WrappingKey wrappingKey, byte[] edek) {
        return wrapOrUnwrap(wrappingKey, edek, "unwrapkey");
    }

    public CompletionStage<GetKeyResponse> getKey(String vaultName, String keyName) {
        Objects.requireNonNull(keyName);
        return service.getBearerToken()
                .thenCompose(bearerToken -> client.sendAsync(getKeyRequest(vaultName, keyName, bearerToken), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)))
                .thenApply(r -> {
                    if (r.statusCode() == 200) {
                        try {
                            GetKeyResponse getKeyResponse = mapper.readValue(r.body(), GetKeyResponse.class);
                            if (getKeyResponse == null) {
                                throw new MalformedResponseBodyException("getKeyResponse is null for key '" + keyName + "'");
                            }
                            return getKeyResponse;
                        }
                        catch (JsonProcessingException e) {
                            throw new MalformedResponseBodyException("failed to parse getKey body for key '" + keyName + "'", e);
                        }
                    }
                    else {
                        throw new UnexpectedHttpStatusCodeException(r);
                    }
                });
    }

    private HttpRequest getKeyRequest(String vaultName, String keyName, BearerToken bearerToken) {
        String getKey = config.keyVaultUrl(vaultName) + "/keys/" + keyName + "?api-version=" + API_VERSION;
        return HttpRequest.newBuilder()
                .header("Authorization", "Bearer " + bearerToken.token())
                .uri(URI.create(getKey)).GET().build();
    }

    private CompletionStage<byte[]> wrapOrUnwrap(WrappingKey wrappingKey, byte[] bytes, String operation) {
        Objects.requireNonNull(wrappingKey);
        Objects.requireNonNull(bytes);
        if (bytes.length == 0) {
            return CompletableFuture.failedFuture(new KmsException("value length is zero for operation " + operation));
        }
        return service.getBearerToken()
                .thenCompose(bearerToken -> client.sendAsync(wrapOrUnwrapKeyRequest(wrappingKey, bytes, bearerToken, operation),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)))
                .thenApply(r -> {
                    if (r.statusCode() == 200) {
                        try {
                            WrapOrUnwrapResponse wrapResponse = mapper.readValue(r.body(), WrapOrUnwrapResponse.class);
                            if (wrapResponse == null) {
                                throw new MalformedResponseBodyException("response body null for key '" + wrappingKey.keyName() + "' for operation " + operation);
                            }
                            return wrapResponse.decodedValue();
                        }
                        catch (JsonProcessingException e) {
                            throw new MalformedResponseBodyException("failed to parse body for key '" + wrappingKey.keyName() + "' for operation " + operation, e);
                        }
                    }
                    else {
                        throw new UnexpectedHttpStatusCodeException(r);
                    }
                });
    }

    private HttpRequest wrapOrUnwrapKeyRequest(WrappingKey wrappingKey, byte[] bytes, BearerToken bearerToken, String operation) {
        String wrapKey = config.keyVaultUrl(wrappingKey.vaultName()) + "/keys/" + wrappingKey.keyName() + "/" + wrappingKey.keyVersion() + "/" + operation
                + "?api-version=" + API_VERSION;
        WrapOrUnwrapRequest value = WrapOrUnwrapRequest.from(wrappingKey.supportedKeyType().getWrapAlgorithm(), bytes);
        try {
            byte[] bodyBytes = mapper.writer().writeValueAsBytes(value);
            return HttpRequest.newBuilder()
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + bearerToken.token())
                    .uri(URI.create(wrapKey)).POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes)).build();
        }
        catch (JsonProcessingException e) {
            throw new KmsException(e);
        }
    }

    @Override
    public void close() {
        service.close();
    }
}
