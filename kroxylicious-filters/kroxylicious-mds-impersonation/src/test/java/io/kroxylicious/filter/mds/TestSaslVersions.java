/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;

final class TestSaslVersions {
    private TestSaslVersions() {
    }

    static ApiVersionsResponseData supported() {
        var versions = new ApiVersionsResponseData.ApiVersionCollection();
        for (ApiKeys key : ApiKeys.values()) {
            if (key.oldestVersion() >= 0) {
                versions.add(new ApiVersionsResponseData.ApiVersion().setApiKey(key.id)
                        .setMinVersion(key.oldestVersion()).setMaxVersion(key.latestVersion()));
            }
        }
        return new ApiVersionsResponseData().setApiKeys(versions);
    }
}
