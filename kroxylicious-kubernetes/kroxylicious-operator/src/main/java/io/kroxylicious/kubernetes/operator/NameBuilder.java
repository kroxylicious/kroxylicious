/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;

/**
 * An unsynchronized
 */
public class NameBuilder {

    private final int maxLength;

    private final StringBuilder buffer = new StringBuilder();

    public NameBuilder(int maxLength) {
        this.maxLength = maxLength;
    }

    public static NameBuilder forRfc1035LabelName() {
        return new NameBuilder(63);
    }

    public static NameBuilder forRfc1123LabelName() {
        return new NameBuilder(63);
    }

    public static NameBuilder forDnsSubdomainName() {
        return new NameBuilder(253);
    }

    public static NameBuilder forResource(Class<? extends HasMetadata> resourceType) {
        if (Service.class.isAssignableFrom(resourceType)) {
            return forRfc1035LabelName();
        }
        else if (Namespace.class.isAssignableFrom(resourceType)) {
            return forRfc1123LabelName();
        }
        else {
            return forDnsSubdomainName();
        }
    }

    public NameBuilder append(String part) {
        buffer.append(part);
        return this;
    }

    public NameBuilder append(char character) {
        buffer.append(character);
        return this;
    }

    public NameBuilder appendNameOf(HasMetadata resource) {
        Optional.ofNullable(resource.getMetadata())
                .map(ObjectMeta::getName)
                .map(buffer::append);
        return this;
    }

    @Override
    public String toString() {
        if (buffer.length() <= maxLength) {
            return buffer.toString();
        }
        else {
            String hash = hash(buffer);
            buffer.setLength(maxLength - 1 - hash.length());
            return buffer.append('-').append(hash).toString();
        }
    }

    private static String hash(StringBuilder buffer) {
        // This is loosely based on the Go code for ComputeHash() from: k8s.io/kubernetes/pkg/controller/controller_utils.go
        int hash = buffer.toString().hashCode();
        var s = Integer.toUnsignedString(hash);
        return safeEncodeString(s);
    }

    private static String safeEncodeString(String s) {
        // This is loosely based on the Go code for SafeEncodeString() from: k8s.io/apimachinery/pkg/util/rand/rand.go
        // SafeEncodeString encodes s using the same characters as rand.String. This reduces the chances of bad words and
        // ensures that strings generated from hash functions appear consistent throughout the API.
        var alphanums = "bcdfghjklmnpqrstvwxz2456789";
        char[] chars = new char[10];
        for (int i = 0; i < s.length(); i++) {
            char b = s.charAt(i);
            chars[i] = alphanums.charAt(((int) b) % alphanums.length());
        }
        return new String(chars, 0, s.length());
    }
}
