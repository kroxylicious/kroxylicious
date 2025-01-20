/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a set of KeyPairs, from which we can select one for each VirtualCluster
 * that has SANs containing the bootstrap host.
 * @param keyPairs
 */
public record KeyPairSet(@JsonProperty(required = true) List<KeyPair> keyPairs) implements KeyProvider {

    @Override
    public <T> T accept(KeyProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }

    // we want to obtain a key pair where all target hostnames match a SAN, literal or wildcard in the certificate
    KeyPair getKeyPair(Set<String> hostnames) {
        List<KeyPair> keyPairStream = keyPairs.stream().filter(k -> k.matchesHostnames(hostnames)).toList();
        if (keyPairStream.isEmpty()) {
            throw new IllegalStateException(
                    "No key pair found with certificate containing VirtualCluster hostnames: " + hostnames + " in SubjectAlternativeNames or common name");
        }
        else if (keyPairStream.size() > 1) {
            throw new IllegalStateException("Multiple key pairs found for hostnames: " + hostnames);
        }
        return keyPairStream.get(0);
    }
}
