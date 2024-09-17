/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.net.http.HttpRequest;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kroxylicious.kms.provider.hashicorp.vault.model.UpdateKeyConfigRequest;
import io.kroxylicious.kms.service.AbstractTestKekManager;

import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.createVaultDelete;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.createVaultGet;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.createVaultPost;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.getBody;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.sendRequest;
import static io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsTestUtils.sendRequestExpectingNoContentResponse;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public class VaultKmsTestKekManager extends AbstractTestKekManager {
    private static final TypeReference<VaultResponse<VaultResponse.ReadKeyData>> VAULT_RESPONSE_READ_KEY_DATA_TYPEREF = new TypeReference<>() {
    };
    private static final String KEYS_PATH = "v1/transit/keys/%s";
    private final URI vaultUrl;

    public VaultKmsTestKekManager(URI vaultUrl) {
        this.vaultUrl = vaultUrl;
    }

    public void create(String keyId) {
        var request = createVaultPost(vaultUrl.resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))), HttpRequest.BodyPublishers.noBody());
        sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);
    }

    public void delete(String keyId) {
        var update = createVaultPost(vaultUrl.resolve((KEYS_PATH + "/config").formatted(encode(keyId, UTF_8))),
                HttpRequest.BodyPublishers.ofString(getBody(new UpdateKeyConfigRequest(true))));
        sendRequest(keyId, update, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);

        var delete = createVaultDelete(vaultUrl.resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))));
        sendRequestExpectingNoContentResponse(delete);
    }

    public VaultResponse.ReadKeyData read(String keyId) {
        var request = createVaultGet(vaultUrl.resolve(KEYS_PATH.formatted(encode(keyId, UTF_8))));
        return sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF).data();
    }

    public void rotate(String keyId) {
        var request = createVaultPost(vaultUrl.resolve((KEYS_PATH + "/rotate").formatted(encode(keyId, UTF_8))), HttpRequest.BodyPublishers.noBody());
        sendRequest(keyId, request, VAULT_RESPONSE_READ_KEY_DATA_TYPEREF);
    }
}
