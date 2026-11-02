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

    private enum Kind {
        TRUNCATABLE,
        FIXED,
        VARIABLE_SUFFIX
    }

    private record Part(Kind kind, String value, int length) {
        Part(Kind kind, String value) {
            this(kind, value, value.length());
        }
    }

    private final int maxLength;

    private final List<Part> parts = new ArrayList<>(5);

    private boolean hasVariableSuffix;

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
        if (hasVariableSuffix) {
            throw new IllegalStateException("cannot append further parts after a variableSuffix");
        }
        parts.add(new Part(canTruncate ? Kind.TRUNCATABLE : Kind.FIXED, part));
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

    /**
     * Reserves {@code maxWidth} characters of this builder's budget for a value that varies across
     * sibling names built from an otherwise-identical sequence of prior calls (for example a node id,
     * or a literal substitution token such as {@code $(nodeId)} intended for a consumer that performs
     * its own substitution later). The value is excluded from the hash computed when truncation is
     * required, and the reserved {@code maxWidth} - not the value's actual length - determines whether
     * truncation is needed and by how much. This guarantees that sibling calls sharing the same
     * preceding parts always truncate and hash identically, regardless of which value is passed here.
     * <p>
     * At most one variable suffix may be added, and it must be the last part appended: it supplies its
     * own leading {@code -}, so callers must not call {@link #separator()} immediately before it.
     *
     * @param value the literal value to append
     * @param maxWidth the width reserved for this value; {@code value} must not exceed it
     * @return this builder
     */
    public NameBuilder variableSuffix(String value, int maxWidth) {
        if (hasVariableSuffix) {
            throw new IllegalStateException("variableSuffix can only be added once");
        }
        if (value.length() > maxWidth) {
            throw new IllegalArgumentException("value '" + value + "' exceeds the reserved maxWidth of " + maxWidth);
        }
        // The leading '-' is folded into the part's value/length so that it's appended
        // consistently whether or not truncation ends up being required.
        parts.add(new Part(Kind.VARIABLE_SUFFIX, "-" + value, maxWidth + 1));
        hasVariableSuffix = true;
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
        // so naively append to sb, but compute `totalTruncatablesLength` in case we do need to
        // truncate. `budget` uses the reserved maxWidth (not the actual value length) for a
        // variable suffix part, so the truncation decision doesn't depend on which value was passed.
        StringBuilder sb = new StringBuilder();
        int totalTruncatablesLength = 0;
        int budget = 0;
        for (Part part : parts) {
            sb.append(part.value());
            budget += part.length();
            if (part.kind() == Kind.TRUNCATABLE) {
                totalTruncatablesLength += part.length();
            }
        }
        if (budget <= maxLength) {
            // Optimism won
            return sb.toString();
        }
        else {
            // Optimism failed. The variable suffix (if any) is excluded from the hash and appended
            // literally after it, so sibling names sharing the same preceding parts always hash
            // and truncate identically regardless of which suffix value was passed.
            List<Part> stableParts = hasVariableSuffix ? parts.subList(0, parts.size() - 1) : parts;
            Part suffixPart = hasVariableSuffix ? parts.get(parts.size() - 1) : null;

            String hash = hash(stableParts);
            int reservedForSuffix = hasVariableSuffix ? suffixPart.length() : 0; // includes the suffix's own leading '-'
            int shortenTo = maxLength - (hash.length() + 1) - reservedForSuffix; // + 1 for the '-' before the hash
            int stableLength = stableParts.stream().mapToInt(Part::length).sum();
            int shortenBy = stableLength - shortenTo;

            StringBuilder result = new StringBuilder();
            if (totalTruncatablesLength == 0) {
                // Edge case: this can only happen if there were no truncatables, or they were all empty
                String stableConcat = stableParts.stream().map(Part::value).collect(Collectors.joining());
                result.append(stableConcat, 0, stableConcat.length() - shortenBy);
            }
            else {
                appendTruncatedTruncatableParts(shortenBy, totalTruncatablesLength, result, stableParts);
            }
            result.append('-').append(hash);
            if (hasVariableSuffix) {
                result.append(suffixPart.value()); // already starts with '-'
            }
            return result.toString();
        }
    }

    private void appendTruncatedTruncatableParts(int shortenBy,
                                                 int totalTruncatablesLength,
                                                 StringBuilder sb,
                                                 List<Part> partsToProcess) {

        // We need to partly truncate the truncatable parts
        // There can be > 1 truncatable part, so we truncate each part by its length-weighted share of the
        // total truncation needed
        record Shortening(int shortenBy, int remainder, int index) {}

        List<Shortening> shortenings = new ArrayList<>(partsToProcess.size());
        int unadjustedTotalShortening = 0;
        for (int i = 0; i < partsToProcess.size(); i++) {
            Part part = partsToProcess.get(i);
            if (part.kind() == Kind.TRUNCATABLE) {
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
        for (int i = 0; i < partsToProcess.size(); i++) {
            Part part = partsToProcess.get(i);
            Shortening shortening = iterator.next();
            String toAppend = part.value();
            if (part.kind() == Kind.TRUNCATABLE) {
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
