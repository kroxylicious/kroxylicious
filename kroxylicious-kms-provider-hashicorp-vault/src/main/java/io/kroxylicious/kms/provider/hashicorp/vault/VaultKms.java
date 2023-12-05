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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@inheritDoc}
 * <br/>
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

    private static final String AES_KEY_ALGO = "AES";

    private final HttpClient vaultClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final URI vaultUrl;
    private final String vaultToken;

    VaultKms(URI vaultUrl, String vaultToken) {
        this.vaultUrl = vaultUrl;
        this.vaultToken = vaultToken;
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
                .uri(vaultUrl.resolve("v1/transit/datakey/plaintext/%s".formatted(kekRef)))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        return vaultClient.sendAsync(request, statusHandler(kekRef, new JsonBodyHandler<Map<String, Map<String, String>>>()))
                .thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(x -> x.get("data"))
                .thenApply(data -> {

                    var plaintext = data.get("plaintext");
                    var ciphertext = data.get("ciphertext");
                    var secretKey = new SecretKeySpec(Base64.getDecoder().decode(plaintext), AES_KEY_ALGO);

                    return new DekPair<>(new VaultEdek(kekRef, ciphertext.getBytes(StandardCharsets.UTF_8)), secretKey);

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

        var body = Map.of("ciphertext", new String(edek.edek(), StandardCharsets.UTF_8));

        HttpRequest request = null;
        try {
            request = createVaultRequest()
                    .uri(vaultUrl.resolve("v1/transit/decrypt/%s".formatted(edek.kekRef())))
                    .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(body)))
                    .build();
        }
        catch (JsonProcessingException e) {
            return CompletableFuture.failedStage(new KmsException("Failed to build request body for %s".formatted(edek.kekRef())));
        }

        return vaultClient.sendAsync(request, statusHandler(edek.kekRef(), new JsonBodyHandler<Map<String, Map<String, String>>>()))
                .thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(x -> x.get("data"))
                .thenApply(data -> {
                    var plaintext = data.get("plaintext");
                    return new SecretKeySpec(Base64.getDecoder().decode(plaintext), AES_KEY_ALGO);
                });
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
                .uri(vaultUrl.resolve("v1/transit/keys/%s".formatted(alias)))
                .build();

        return vaultClient.sendAsync(request, statusHandler(alias, new JsonBodyHandler<Map<String, Map<String, String>>>()))
                .thenApply(HttpResponse::body)
                .thenApply(Supplier::get)
                .thenApply(x -> x.get("data"))
                .thenApply(data -> data.get("name"));

    }

    @NonNull
    @Override
    public Serde<VaultEdek> edekSerde() {
        return new VaultEdekSerde();
    }

    private HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                .header("X-Vault-Token", vaultToken)
                .header("Accept", "application/json");
    }

    private static <T> HttpResponse.BodyHandler<T> statusHandler(String keyRef, HttpResponse.BodyHandler<T> handler) {
        return r -> {

            if (r.statusCode() == 404 || r.statusCode() == 400) {
                throw new UnknownKeyException("key '%s' is not found.".formatted(keyRef));
            }
            else if (r.statusCode() != 200) {
                throw new KmsException("fail to retrieve key '%s', HTTP status code %d.".formatted(keyRef, r.statusCode()));
            }
            return handler.apply(r);
        };
    }
}
