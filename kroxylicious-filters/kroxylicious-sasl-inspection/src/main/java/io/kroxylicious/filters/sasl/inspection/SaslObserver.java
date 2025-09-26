/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Set;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;

public interface SaslObserver {

    Set<String> SUPPORTED_MECHANISMS = Set.of(
            Mech.PLAIN.mechanismName(),
            Mech.SCRAM_SHA_256.mechanismName(),
            Mech.SCRAM_SHA_512.mechanismName());

    static SaslObserver fromMechanismName(String mechanism) {
        return Mech.valueOf(mechanism.replace('-', '_'));
    }

    boolean isLastSaslAuthenticateResponse(int numAuthenticateSeen);

    String mechanismName();

    boolean isFinished(int numAuthenticateSeen);

    String authorizationId(SaslAuthenticateRequestData request);

    boolean requestContainsAuthorizationId(int numAuthenticateSeen);
}
