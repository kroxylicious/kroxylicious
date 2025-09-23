/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.foo;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authorization.Subject;
import io.kroxylicious.proxy.authorization.SubjectBuilder;

public class SaslSubjectBuilder implements SubjectBuilder {
    @Override
    public CompletionStage<Subject> buildSubject(Context context) {
        return CompletableFuture.completedStage(
                context.saslAuthorizedId()
                        .map(id -> new Subject(Set.of(new SaslAuthorizedId(id))))
                        .orElse(Subject.ANONYMOUS));
    }
}
