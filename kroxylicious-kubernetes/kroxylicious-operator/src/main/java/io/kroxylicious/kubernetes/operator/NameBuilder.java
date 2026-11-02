/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;

/**
 * An unsynchronized builder (similar to {@link StringBuilder}) for Kubernetes names which
 * honours the various length constraints imposed for different kinds.
 * This does not validate the syntax of the names which get built.
 */
public class NameBuilder {

    private record Part(boolean truncatable, String value, int length) {
        Part(boolean truncatable, String value) {
            this(truncatable, value, value.length());
        }
    }

    private final int maxLength;

    private final List<Part> parts = new ArrayList<>(5);

    public NameBuilder(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * Return a builder that's suitable for resources (Services, specifically)
     * whose names must be RFC 1035 subdomain names (max length 63 characters).
     * @return A builder for names of that type
     */
    public static NameBuilder forRfc1035LabelName() {
        return new NameBuilder(63);
    }

    /**
     * Return a builder that's suitable for resources
     * whose names must be RFC 1123 subdomain names (max length 63 characters).
     * @return A builder for names of that type
     */
    public static NameBuilder forRfc1123LabelName() {
        return new NameBuilder(63);
    }

    /**
     * Return a builder that's suitable for resources
     * whose names must be DNS subdomain names (max length 253 characters).
     * @return A builder for names of that type
     */
    public static NameBuilder forDnsSubdomainName() {
        return new NameBuilder(253);
    }

    /**
     * Return a builder that's suitable for a resource of the given type.
     * @param resourceType The resource type
     * @return A builder for names of that type
     */
    public static NameBuilder forResource(Class<? extends HasMetadata> resourceType) {
        if (Service.class.isAssignableFrom(resourceType)) {
            return forRfc1035LabelName();
        }
        else if (Namespace.class.isAssignableFrom(resourceType)
                || Node.class.isAssignableFrom(resourceType)) {
            return forRfc1123LabelName();
        }
        else {
            return forDnsSubdomainName();
        }
    }

    /**
     * Append the given string
     * @param part The string to append
     * @return This builder
     */
    private NameBuilder append(boolean canTruncate, String part) {
        parts.add(new Part(canTruncate, part));
        return this;
    }

    /**
     * Append the given string
     * @param part The string to append
     * @return This builder
     */
    public NameBuilder append(String part) {
        append(true, part);
        return this;
    }

    public NameBuilder separator() {
        append(false, "-");
        return this;
    }

    /**
     * Append the given string
     * @param part The string to append
     * @return This builder
     */
    public NameBuilder role(String part) {
        append(false, part);
        return this;
    }

    public NameBuilder placeholder(String part, int maxLength) {
        parts.add(new Part(false, part, maxLength));
        return this;
    }

    /**
     * Append the {@code metadata.name} of the given resource.
     * @param resource The resource
     * @return This builder
     */
    public NameBuilder appendNameOf(HasMetadata resource) {
        Optional.ofNullable(resource.getMetadata())
                .map(ObjectMeta::getName)
                .ifPresent(this::append);
        return this;
    }

    /**
     * Return the name that's been constructed from calls to {@code append()} methods.
     * @return the name
     */
    @Override
    public String toString() {
        // First pass: Optimistically assume the name will be short enough to not need truncation
        // so naively append to sb, but compute `totalTruncatablesLength` in case
        // we do need to truncate.
        StringBuilder sb = new StringBuilder();
        int totalTruncatablesLength = 0;
        for (Part part : parts) {
            sb.append(part.value());
            if (part.truncatable()) {
                totalTruncatablesLength += part.length();
            }
        }
        if (sb.length() <= maxLength) {
            // Optimism won
            return sb.toString();
        }
        else {
            // Optimism failed
            String hash = hash(parts);
            int shortenTo = maxLength - (hash.length() + 1); // + 1 for the '-' separator
            int shortenBy = sb.length() - shortenTo;

            if (totalTruncatablesLength == 0) {
                // Edge case: this can only happen if there were no truncatables, or they were all empty
                sb.setLength(sb.length() - shortenBy);
            }
            else {
                sb.setLength(0);
                appendTruncatedTruncatableParts(shortenBy, totalTruncatablesLength, sb);
            }
            return sb.append('-').append(hash).toString();
        }
    }

    private void appendTruncatedTruncatableParts(int shortenBy,
                                                 int totalTruncatablesLength,
                                                 StringBuilder sb) {

        // We need to partly truncate the truncatable parts
        // There can be > 1 truncatable part, so we truncate each part by its length-weighted share of the
        // total truncation needed
        record Shortening(int shortenBy, int remainder, int index) {}

        List<Shortening> shortenings = new ArrayList<>(parts.size());
        int unadjustedTotalShortening = 0;
        for (int i = 0; i < parts.size(); i++) {
            Part part = parts.get(i);
            if (part.truncatable()) {
                // TODO what if none of the parts is truncatable?
                // Truncate each part by its length-weighted share of the total truncation needed
                // TODO division by zero
                int shortenByThisPart = (shortenBy * part.length()) / totalTruncatablesLength;
                unadjustedTotalShortening += shortenByThisPart;
                // We adjust for rounding by apportioning an extra character's truncation to those
                // truncatable parts with the largest remainder.
                int remainder = part.length() % totalTruncatablesLength;
                shortenings.add(new Shortening(shortenByThisPart, remainder, i));
            }
            else {
                // Don't allocate a Shortening for non-truncatable parts
                shortenings.add(null);
            }
        }

        int numAdjustments = shortenBy - unadjustedTotalShortening;
        Set<Integer> indexWithLargeRemainder = shortenings.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.<Shortening, Integer> comparing(Shortening::remainder).reversed())
                .limit(numAdjustments)
                .map(Shortening::index)
                .collect(Collectors.toSet());

        // Re-compute the name, truncating each truncatable part by its fair share
        Iterator<Shortening> iterator = shortenings.iterator();
        for (int i = 0; i < parts.size(); i++) {
            Part part = parts.get(i);
            Shortening shortening = iterator.next();
            String toAppend = part.value();
            if (part.truncatable()) {
                int end = toAppend.length() - shortening.shortenBy();
                if (indexWithLargeRemainder.contains(i)) {
                    end -= 1;
                }
                sb.append(toAppend, 0, end);
            }
            else {
                sb.append(toAppend);
            }
        }
    }

    private static String hash(List<Part> parts) {
        // This is loosely based on the Go code for ComputeHash() from: k8s.io/kubernetes/pkg/controller/controller_utils.go
        Fnv1a32 fnv1a32 = new Fnv1a32();
        parts.forEach(part -> fnv1a32.update(part.value));
        int hash = fnv1a32.digest();
        var s = Integer.toUnsignedString(hash);
        return safeEncodeString(s);
    }

    static String safeEncodeString(String s) {
        // This is loosely based on the Go code for SafeEncodeString() from: k8s.io/apimachinery/pkg/util/rand/rand.go
        // SafeEncodeString encodes s using the same characters as rand.String. This reduces the chances of bad words and
        // ensures that strings generated from hash functions appear consistent throughout the API.

        // Note: The kubernetes algorithm means that `s` contains only the decimal digits 0-9
        // which means that only chars from "456789bcdf" can ever be returned.
        // We choose to follow k8s rather than aim for maximum encoding efficiency.
        var alphanums = "bcdfghjklmnpqrstvwxz2456789";
        char[] chars = new char[s.length()];
        for (int i = 0; i < s.length(); i++) {
            char b = s.charAt(i);
            chars[i] = alphanums.charAt(b % alphanums.length());
        }

        return new String(chars, 0, s.length());
    }

    static class Fnv1a32 {

        private int hash;

        Fnv1a32() {
            this.hash = 0x811c9dc5; // 32-bit FNV offset basis
        }

        public void update(CharSequence str) {
            for (int i = 0; i < str.length(); i++) {
                hash ^= (str.charAt(i) & 0xFF); // Mask to 8-bit ASCII
                hash *= 0x01000193; // 32-bit FNV prime
            }
        }

        public int digest() {
            return hash;
        }
    }
}
