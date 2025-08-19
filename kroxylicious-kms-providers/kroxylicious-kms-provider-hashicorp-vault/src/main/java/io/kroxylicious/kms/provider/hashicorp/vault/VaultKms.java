/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DataKeyData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DecryptData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of the KMS interface backed by a remote instance of HashiCorp Vault (v1).
 */
public class VaultKms implements Kms<String, VaultEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AES_KEY_ALGO = "AES";
    private static final Pattern LEGAL_API_VERSION_REGEX = Pattern.compile("^/?v1/.+");
    private static final TypeReference<VaultResponse<DataKeyData>> DATA_KEY_DATA_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<VaultResponse<ReadKeyData>> READ_KEY_DATA_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<VaultResponse<DecryptData>> DECRYPT_DATA_TYPE_REF = new TypeReference<>() {
    };
    private final Duration timeout;
    private final HttpClient vaultClient;

    /**
     * The vault url which will include the path to the transit engine.
     */
    private final URI vaultTransitEngineUrl;
    private final String vaultToken;

    VaultKms(URI vaultTransitEngineUrl,
             String vaultToken,
             Duration timeout,
             UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        Objects.requireNonNull(vaultTransitEngineUrl);
        Objects.requireNonNull(vaultToken);
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(tlsConfigurator);
        this.vaultTransitEngineUrl = ensureEndsInSlash(validateTransitPath(vaultTransitEngineUrl));
        this.vaultToken = vaultToken;
        this.timeout = timeout;
        this.vaultClient = createClient(tlsConfigurator);
    }

    private URI validateTransitPath(URI vaultTransitEngineUrl) {
        String path = vaultTransitEngineUrl.getPath();
        if (path == null || !LEGAL_API_VERSION_REGEX.matcher(path).matches()) {
            throw new IllegalArgumentException(("vaultTransitEngineUrl path (%s) must start with v1/ and must specify the complete path to"
                    + " the transit engine e.g 'v1/transit' or 'v1/mynamespace/transit' if namespaces are in use.").formatted(path));
        }
        return vaultTransitEngineUrl;
    }

    private URI ensureEndsInSlash(URI uri) {
        // We use #resolve to build the urls for the endpoints, so the path must end in a slash.
        return uri.resolve(uri.getPath() + "/").normalize();
    }

    private HttpClient createClient(UnaryOperator<HttpClient.Builder> tlsConfigurator) {
        return tlsConfigurator.apply(HttpClient.newBuilder())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    @VisibleForTesting
    HttpClient getHttpClient() {
        return vaultClient;
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#generate-data-key">https://developer.hashicorp.com/vault/api-docs/secret/transit#generate-data-key</a>
     */
    @Override
    public CompletionStage<DekPair<VaultEdek>> generateDekPair(String kekRef) {

        var request = createVaultRequest()
                .uri(vaultTransitEngineUrl.resolve("datakey/plaintext/%s".formatted(encode(kekRef, UTF_8))))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        return sendAsync(kekRef, request, DATA_KEY_DATA_TYPE_REF, UnknownKeyException::new)
                .thenApply(data -> {
                    var secretKey = DestroyableRawSecretKey.takeOwnershipOf(data.plaintext(), AES_KEY_ALGO);
                    return new DekPair<>(new VaultEdek(kekRef, data.ciphertext().getBytes(UTF_8)), secretKey);
                });

    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#decrypt">https://developer.hashicorp.com/vault/api-docs/secret/transit#decrypt</a>
     */
    @Override
    public CompletionStage<SecretKey> decryptEdek(VaultEdek edek) {

        var body = createDecryptPostBody(edek);

        var request = createVaultRequest()
                .uri(vaultTransitEngineUrl.resolve("decrypt/%s".formatted(encode(edek.kekRef(), UTF_8))))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return sendAsync(edek.kekRef(), request, DECRYPT_DATA_TYPE_REF, UnknownKeyException::new)
                .thenApply(data -> DestroyableRawSecretKey.takeOwnershipOf(data.plaintext(), AES_KEY_ALGO));
    }

    private String createDecryptPostBody(VaultEdek edek) {
        var map = Map.of("ciphertext", new String(edek.edek(), UTF_8));

        try {
            return OBJECT_MAPPER.writeValueAsString(map);
        }
        catch (JsonProcessingException e) {
            throw new KmsException("Failed to build request body for %s".formatted(edek.kekRef()));
        }
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#read-key">https://developer.hashicorp.com/vault/api-docs/secret/transit#read-key</a>
     */
    @Override
    public CompletableFuture<String> resolveAlias(String alias) {

        var request = createVaultRequest()
                .uri(vaultTransitEngineUrl.resolve("keys/%s".formatted(encode(alias, UTF_8))))
                .build();
        return sendAsync(alias, request, READ_KEY_DATA_TYPE_REF, UnknownAliasException::new)
                .thenApply(ReadKeyData::name);
    }

    private <T> CompletableFuture<T> sendAsync(String key,
                                               HttpRequest request,
                                               TypeReference<VaultResponse<T>> valueTypeRef,
                                               Function<String, KmsException> exception) {
        return vaultClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> checkResponseStatus(key, response, exception))
                .thenApply(HttpResponse::body)
                .thenApply(bytes -> decodeJson(valueTypeRef, bytes))
                .thenApply(VaultResponse::data);
    }

    private static <T> VaultResponse<T> decodeJson(TypeReference<VaultResponse<T>> valueTypeRef, byte[] bytes) {
        try {
            VaultResponse<T> result = OBJECT_MAPPER.readValue(bytes, valueTypeRef);
            Arrays.fill(bytes, (byte) 0);
            return result;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static HttpResponse<byte[]> checkResponseStatus(String key,
                                                            HttpResponse<byte[]> response,
                                                            Function<String, KmsException> notFound) {
        if (response.statusCode() == 404 || response.statusCode() == 400) {
            throw notFound.apply("key '%s' is not found.".formatted(key));
        }
        else if (response.statusCode() != 200) {
            throw new KmsException("fail to retrieve key '%s', HTTP status code %d.".formatted(key, response.statusCode()));
        }
        return response;
    }

    @Override
    public Serde<VaultEdek> edekSerde() {
        return VaultEdekSerde.instance();
    }

    @VisibleForTesting
    HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                .timeout(timeout)
                .header("X-Vault-Token", vaultToken)
                .header("Accept", "application/json");
    }

    @VisibleForTesting
    URI getVaultTransitEngineUri() {
        return vaultTransitEngineUrl;
    }
}
