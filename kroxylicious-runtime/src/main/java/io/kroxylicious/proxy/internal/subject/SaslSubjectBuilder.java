/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.subject;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.Subject;

public class SaslSubjectBuilder implements SubjectBuilder {

    @Override
    public CompletionStage<Subject> buildSubject(Context context) {
        return CompletableFuture.completedStage(
                context.clientSaslContext()
                        .map(saslContext -> new Subject(Set.of(new User(saslContext.authorizationId()))))
                        .orElse(Subject.ANONYMOUS));
    }
}
