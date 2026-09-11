/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for a single {@code map} entry of a principal adder, defining one mapping rule.
 * Exactly one of {@code replaceMatch} and {@code else} must be given.
 *
 * @param replaceMatch a sed-like replacement expression, see {@link ReplaceMatchMappingRule}.
 * @param else_ a fallback mapping, either {@code identity} or {@code anonymous}.
 */
public record Map(@Nullable String replaceMatch,
                  @JsonProperty("else") @Nullable String else_) {
    /**
     * Validates that exactly one of {@code replaceMatch} and {@code else} is given and is well-formed.
     */
    public Map {
        if (replaceMatch != null) {
            if (else_ != null) {
                throw new IllegalArgumentException("`replaceMatch` and `else` are mutually exclusive.");
            }
            new ReplaceMatchMappingRule(replaceMatch);
        }
        else if (else_ == null) {
            throw new IllegalArgumentException("Exactly one of `replaceMatch` and `else` are required.");
        }
        else if (!DefaultSaslSubjectBuilderService.ELSE_IDENTITY.equals(else_)
                && !DefaultSaslSubjectBuilderService.ELSE_ANONYMOUS.equals(else_)) {
            throw new IllegalArgumentException("`else` can only take the value 'identity' or 'anonymous'.");
        }
    }
}
