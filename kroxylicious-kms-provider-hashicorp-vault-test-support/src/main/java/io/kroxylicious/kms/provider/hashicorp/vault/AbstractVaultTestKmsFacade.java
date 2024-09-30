/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreatePolicyRequest;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreateTokenRequest;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreateTokenResponse;
import io.kroxylicious.kms.provider.hashicorp.vault.model.EnableEngineRequest;
import io.kroxylicious.kms.provider.hashicorp.vault.model.UpdateKeyConfigRequest;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final TypeReference<CreateTokenResponse> VAULT_RESPONSE_CREATE_TOKEN_RESPONSE_TYPEREF = new TypeReference<>() {
    };
    private static final TypeReference<VaultResponse<VaultResponse.ReadKeyData>> VAULT_RESPONSE_READ_KEY_DATA_TYPEREF = new TypeReference<>() {
    };
    private static final String KEYS_PATH = "v1/transit/keys/%s";
    private String kmsVaultToken;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient vaultClient = HttpClient.newHttpClient();
    public static final String VAULT_ROOT_TOKEN = "rootToken";

    protected AbstractVaultTestKmsFacade() {
    }

    protected abstract void startVault();

    protected abstract void stopVault();

    @Override
    public final void start() {

        startVault();

        // enable transit engine
        enableTransit();

        // create policy
        var policyName = "kroxylicious_encryption_filter_policy";
        try (var policyStream = AbstractVaultTestKmsFacade.class.getResourceAsStream("kroxylicious_encryption_filter_policy.hcl")) {
            createPolicy(policyName, policyStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create policy", e);
        }
        // create least-privilege vault token that KMS will use. The decision to use orphan tokens
        // (https://developer.hashicorp.com/vault/docs/concepts/tokens#token-hierarchies-and-orphan-tokens)
        // but no functional impact. A child token would work too.
        kmsVaultToken = createOrphanToken("kroxylicious_encryption_filter", true, Set.of(policyName));
    }

    protected void enableTransit() {
        var engine = new EnableEngineRequest("transit");
        var body = encodeJson(engine);
        var request = createVaultPost(getVaultUrl().resolve("v1/sys/mounts/transit"), HttpRequest.BodyPublishers.ofString(body));

        sendRequestExpectingNoContentResponse(request);
    }

    protected void createPolicy(String policyName, InputStream policyStream) {
        Objects.requireNonNull(policyName);
        Objects.requireNonNull(policyStream);
        var createPolicy = encodeJson(CreatePolicyRequest.fromInputStream(policyStream));
        var request = createVaultPost(getVaultUrl().resolve("v1/sys/policy/%s".formatted(encode(policyName, UTF_8))), HttpRequest.BodyPublishers.ofString(createPolicy));

        sendRequestExpectingNoContentResponse(request);
    }

    protected String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies) {
        var token = new CreateTokenRequest(description, noDefaultPolicy, policies);
        String body = encodeJson(token);
        var request = createVaultPost(getVaultUrl().resolve("v1/auth/token/create-orphan"), HttpRequest.BodyPublishers.ofString(body));

        return sendRequest("dummy", request, VAULT_RESPONSE_CREATE_TOKEN_RESPONSE_TYPEREF).auth().clientToken();
    }

    @NonNull
    protected abstract URI getVaultUrl();

    @Override
    public final Config getKmsServiceConfig() {
        return new Config(getVaultTransitEngineUrl(), new InlinePassword(kmsVaultToken), null);
    }

    @Override
    public final Class<VaultKmsService> getKmsServiceClass() {
        return VaultKmsService.class;
    }

    @Override
    public final void stop() {
        stopVault();
    }

    @NonNull
    protected final URI getVaultTransitEngineUrl() {
        return getVaultUrl().resolve("v1/transit/");
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new VaultKmsTestKekManager();
    }

    class VaultKmsTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String keyId) {
            var request = createVaultPost(getVaultUrl().resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))), HttpRequest.BodyPublishers.noBody());
            sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);
        }

        @Override
        public void deleteKek(String keyId) {
            var update = createVaultPost(getVaultUrl().resolve((KEYS_PATH + "/config").formatted(encode(keyId, UTF_8))),
                    HttpRequest.BodyPublishers.ofString(encodeJson(new UpdateKeyConfigRequest(true))));
            sendRequest(keyId, update, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);

            var delete = createVaultDelete(getVaultUrl().resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))));
            sendRequestExpectingNoContentResponse(delete);
        }

        @Override
        public VaultResponse.ReadKeyData read(String keyId) {
            var request = createVaultGet(getVaultUrl().resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))));
            return sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF).data();
        }

        @Override
        public void rotateKek(String keyId) {
            var request = createVaultPost(getVaultUrl().resolve((KEYS_PATH + "/rotate").formatted(encode(keyId, UTF_8))), HttpRequest.BodyPublishers.noBody());
            sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);
        }
    }

    private HttpRequest createVaultGet(URI url) {
        return createVaultRequest()
                .uri(url)
                .GET()
                .build();
    }

    private HttpRequest createVaultDelete(URI url) {
        return createVaultRequest()
                .uri(url)
                .DELETE()
                .build();
    }

    private HttpRequest createVaultPost(URI url, HttpRequest.BodyPublisher bodyPublisher) {
        return createVaultRequest()
                .uri(url)
                .POST(bodyPublisher).build();
    }

    private HttpRequest.Builder createVaultRequest() {
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
