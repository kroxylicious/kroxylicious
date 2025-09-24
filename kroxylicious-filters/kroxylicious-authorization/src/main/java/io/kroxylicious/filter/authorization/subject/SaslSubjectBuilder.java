/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.authorization.subject;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.authorizer.service.Subject;

public class SaslSubjectBuilder implements ClientSubjectBuilder {
    @Override
    public CompletionStage<Subject> buildSubject(Context context) {
        return CompletableFuture.completedStage(
                context.saslAuthorizedId()
                        .map(id -> new Subject(Set.of(new User(id))))
                        .orElse(Subject.ANONYMOUS));
    }
}
