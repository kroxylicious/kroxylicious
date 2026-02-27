/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public record MapperContext(Subject authenticatedSubject, @Nullable ClientTlsContext clientTlsContext, @Nullable ClientSaslContext clientSaslContext) {}
