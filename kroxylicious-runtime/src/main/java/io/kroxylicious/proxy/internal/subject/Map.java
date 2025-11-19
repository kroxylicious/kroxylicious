/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

public record Map(@Nullable String replaceMatch,
                  @JsonProperty("else") @Nullable String else_) {
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
        else if (!else_.equals(DefaultSaslSubjectBuilderService.ELSE_IDENTITY)
                && !else_.equals(DefaultSaslSubjectBuilderService.ELSE_ANONYMOUS)) {
            throw new IllegalArgumentException("`else` can only take the value 'identity' or 'anonymous'.");
        }
    }
}
