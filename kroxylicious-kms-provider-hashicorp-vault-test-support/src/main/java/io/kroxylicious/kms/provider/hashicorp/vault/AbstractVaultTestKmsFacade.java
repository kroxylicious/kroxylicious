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
import java.net.http.HttpRequest;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreatePolicyRequest;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreateTokenRequest;
import io.kroxylicious.kms.provider.hashicorp.vault.model.CreateTokenResponse;
import io.kroxylicious.kms.provider.hashicorp.vault.model.EnableEngineRequest;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.createVaultPost;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.getBody;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.sendRequest;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.sendRequestExpectingNoContentResponse;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final TypeReference<CreateTokenResponse> VAULT_RESPONSE_CREATE_TOKEN_RESPONSE_TYPEREF = new TypeReference<>() {
    };
    private String kmsVaultToken;

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
        var body = getBody(engine);
        var request = createVaultPost(getVaultUrl().resolve("v1/sys/mounts/transit"), HttpRequest.BodyPublishers.ofString(body));

        sendRequestExpectingNoContentResponse(request);
    }

    protected void createPolicy(String policyName, InputStream policyStream) {
        Objects.requireNonNull(policyName);
        Objects.requireNonNull(policyStream);
        var createPolicy = getBody(CreatePolicyRequest.fromInputStream(policyStream));
        var request = createVaultPost(getVaultUrl().resolve("v1/sys/policy/%s".formatted(encode(policyName, UTF_8))), HttpRequest.BodyPublishers.ofString(createPolicy));

        sendRequestExpectingNoContentResponse(request);
    }

    protected String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies) {
        var token = new CreateTokenRequest(description, noDefaultPolicy, policies);
        String body = getBody(token);
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
        return new VaultKmsTestKekManager(getVaultUrl());
    }
}
