/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.secret;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A PasswordProvider is an abstract source of a password.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(InlinePassword.class), @JsonSubTypes.Type(FilePassword.class) })
@SuppressWarnings("java:S5738") // java:S5738 warns of the use of the deprecated class.
public interface PasswordProvider {
    String getProvidedPassword();
}
