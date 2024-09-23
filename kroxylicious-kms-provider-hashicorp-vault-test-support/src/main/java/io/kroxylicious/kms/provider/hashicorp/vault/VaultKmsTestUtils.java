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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;

/**
 * The type Vault kms test utils.
 */
public class VaultKmsTestUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient vaultClient = HttpClient.newHttpClient();
    public static final String VAULT_ROOT_TOKEN = "rootToken";

    private VaultKmsTestUtils() {
    }

    /**
     * Create vault get http request.
     *
     * @param url the url
     * @return the http request
     */
    public static HttpRequest createVaultGet(URI url) {
        return createVaultRequest()
                .uri(url)
                .GET()
                .build();
    }

    /**
     * Create vault delete http request.
     *
     * @param url the url
     * @return the http request
     */
    public static HttpRequest createVaultDelete(URI url) {
        return createVaultRequest()
                .uri(url)
                .DELETE()
                .build();
    }

    /**
     * Create vault post http request.
     *
     * @param url the url
     * @param bodyPublisher the body publisher
     * @return the http request
     */
    public static HttpRequest createVaultPost(URI url, HttpRequest.BodyPublisher bodyPublisher) {
        return createVaultRequest()
                .uri(url)
                .POST(bodyPublisher).build();
    }

    private static HttpRequest.Builder createVaultRequest() {
        return HttpRequest.newBuilder()
                .header("X-Vault-Token", VAULT_ROOT_TOKEN)
                .header("Accept", "application/json");
    }

    /**
     * Send request.
     *
     * @param <R>  the type parameter
     * @param key the key
     * @param request the request
     * @param valueTypeRef the value type ref
     * @return the typeRef
     */
    public static <R> R sendRequest(String key, HttpRequest request, TypeReference<R> valueTypeRef) {
        try {
            HttpResponse<byte[]> response = vaultClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 404) {
                throw new UnknownAliasException(key);
            }
            else if (response.statusCode() != 200) {
                throw new IllegalStateException("unexpected response %s for request: %s".formatted(response.statusCode(), request.uri()));
            }
            byte[] body = response.body();
            return decodeJson(valueTypeRef, body);
        }
        catch (IOException e) {
            if (e.getCause() instanceof KmsException ke) {
                throw ke;
            }
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during REST API call : %s".formatted(request.uri()), e);
        }
    }

    /**
     * Send request expecting no content response.
     *
     * @param request the request
     */
    public static void sendRequestExpectingNoContentResponse(HttpRequest request) {
        try {
            var response = vaultClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 204) {
                throw new IllegalStateException("Unexpected response : %d to request %s".formatted(response.statusCode(), request.uri()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Gets body json.
     *
     * @param obj the object
     * @return the body
     */
    public static String encodeJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode the request body", e);
        }
    }

    /**
     * Decode json
     *
     * @param <T>  the type parameter
     * @param valueTypeRef the value type ref
     * @param bytes the bytes
     * @return the type ref
     */
    public static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
