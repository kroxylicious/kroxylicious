/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;

class AuthorizedOps {

    private AuthorizedOps() {
    }

    public static int topicAuthorizedOps(AuthorizeResult authorize, int upstreamAuthorizedOps, String topicName) {
        int enforcedAuthorizedOps = 0;
        for (var clusterOp : TopicResource.values()) {
            if (authorize.decision(clusterOp, topicName) == Decision.ALLOW) {
                enforcedAuthorizedOps |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        // each operation must be allowed both upstream and by this Enforcement
        return enforcedAuthorizedOps & upstreamAuthorizedOps;
    }

    public static int clusterAuthorizedOps(AuthorizeResult authorize, int upstreamAuthorizedOps) {
        int enforcedAuthorizedOps = 0;
        for (var clusterOp : ClusterResource.values()) {
            if (authorize.decision(clusterOp, "") == Decision.ALLOW) {
                enforcedAuthorizedOps |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        // each operation must be allowed both upstream and by this Enforcement
        return enforcedAuthorizedOps & upstreamAuthorizedOps;
    }
}
