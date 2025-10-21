/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import io.kroxylicious.filter.usernamespace.UserNamespace.ResourceType;
import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface ResourceNameMapper {
    String map(Subject authenticateSubject, @Nullable ClientTlsContext clientTlsContext, @Nullable ClientSaslContext clientSaslContext, ResourceType resourceType,
               String unmappedResourceName);

    String unmap(Subject authenticateSubject, @Nullable ClientTlsContext clientTlsContext, @Nullable ClientSaslContext clientSaslContext, ResourceType resourceType,
                 String mappedResourceName);

    boolean isInNamespace(Subject authenticateSubject, @Nullable ClientTlsContext clientTlsContext, @Nullable ClientSaslContext clientSaslContext,
                          ResourceType resourceType, String mappedResourceName);
}
