/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DataKeyData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DecryptData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of the KMS interface backed by a remote instance of HashiCorp Vault (v1).
 * <br/>
 * <h2>TODO</h2>
 * <ul>
 *    <li>don't assume /transit endpoint</li>
 *    <li>HTTPs</li>
 *    <li>Client trust store</li>
 *    <li>Securely pass vault token</li>
 * </ul>
 */
public class VaultKms implements Kms<String, VaultEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AES_KEY_ALGO = "AES";
    private final Duration timeout;
    private final HttpClient vaultClient;
    private final URI vaultUrl;
    private final String vaultToken;

    VaultKms(URI vaultUrl, String vaultToken, Duration timeout) {
        this.vaultUrl = vaultUrl;
        this.vaultToken = vaultToken;
        this.timeout = timeout;
        vaultClient = createClient();
    }

    private HttpClient createClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(timeout)
                .build();
    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#generate-data-key">https://developer.hashicorp.com/vault/api-docs/secret/transit#generate-data-key</a>
     */
    @NonNull
    @Override
    public CompletionStage<DekPair<VaultEdek>> generateDekPair(@NonNull String kekRef) {
        var request = createVaultRequest()
                .uri(vaultUrl.resolve("v1/transit/datakey/plaintext/%s".formatted(encode(kekRef, UTF_8))))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        Metrics.createDataKey().attempt().increment();
        return vaultClient.sendAsync(request, statusHandler(kekRef, new JsonBodyHandler<VaultResponse<DataKeyData>>(new TypeReference<>() {
        }), UnknownKeyException::new))
                .thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(VaultResponse::data)
                .thenApply(data -> {
                    var secretKey = new SecretKeySpec(Base64.getDecoder().decode(data.plaintext()), AES_KEY_ALGO);
                    return new DekPair<>(new VaultEdek(kekRef, data.ciphertext().getBytes(UTF_8)), secretKey);
                }).whenComplete((vaultEdekDekPair, throwable) -> {
                    recordMetrics(throwable, Metrics.createDataKey());
                });

    }

    /**
     * {@inheritDoc}
     * <br/>
     * @see <a href="https://developer.hashicorp.com/vault/api-docs/secret/transit#decrypt">https://developer.hashicorp.com/vault/api-docs/secret/transit#decrypt</a>
     */
    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(@NonNull VaultEdek edek) {

        var body = createDecryptPostBody(edek);

        Metrics.decryptEdek().attempt().increment();
        var request = createVaultRequest()
                .uri(vaultUrl.resolve("v1/transit/decrypt/%s".formatted(encode(edek.kekRef(), UTF_8))))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return vaultClient.sendAsync(request, statusHandler(edek.kekRef(), new JsonBodyHandler<VaultResponse<DecryptData>>(new TypeReference<>() {
        }), UnknownKeyException::new)).thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(VaultResponse::data)
                .<SecretKey> thenApply(data -> new SecretKeySpec(Base64.getDecoder().decode(data.plaintext()), AES_KEY_ALGO))
                .whenComplete((secretKeySpec, throwable) -> {
                    recordMetrics(throwable, Metrics.decryptEdek());
                });
    }

    private String createDecryptPostBody(@NonNull VaultEdek edek) {
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
    @NonNull
    @Override
    public CompletableFuture<String> resolveAlias(@NonNull String alias) {

        var request = createVaultRequest()
                .uri(vaultUrl.resolve("v1/transit/keys/%s".formatted(encode(alias, UTF_8))))
                .build();

        Metrics.resolveAlias().attempt().increment();
        return vaultClient.sendAsync(request, statusHandler(alias, new JsonBodyHandler<VaultResponse<ReadKeyData>>(new TypeReference<>() {
        }), UnknownAliasException::new))
                .thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(VaultResponse::data)
                .thenApply(ReadKeyData::name)
                .whenComplete((s, throwable) -> {
                    recordMetrics(throwable, Metrics.resolveAlias());
                });

    }

    @NonNull
    @Override
    public Serde<VaultEdek> edekSerde() {
        return new VaultEdekSerde();
    }

    private HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                .timeout(timeout)
                .header("X-Vault-Token", vaultToken)
                .header("Accept", "application/json");
    }

    private static void recordMetrics(Throwable throwable, Metrics.OperationMetrics outcome) {
        if (throwable == null) {
            outcome.success().increment();
        }
        else {
            Throwable toCheck = throwable instanceof CompletionException && throwable.getCause() != null ? throwable.getCause() : throwable;
            if (toCheck instanceof UnknownAliasException || toCheck instanceof UnknownKeyException) {
                outcome.notFound().increment();
            }
            else {
                outcome.exception().increment();
            }
        }
    }

    private static <T> HttpResponse.BodyHandler<T> statusHandler(String keyRef, HttpResponse.BodyHandler<T> handler, Function<String, KmsException> notFound) {
        return r -> {
            if (r.statusCode() == 404 || r.statusCode() == 400) {
                throw notFound.apply("key '%s' is not found.".formatted(keyRef));
            }
            else if (r.statusCode() != 200) {
                throw new KmsException("fail to retrieve key '%s', HTTP status code %d.".formatted(keyRef, r.statusCode()));
            }
            return handler.apply(r);
        };
    }
}
