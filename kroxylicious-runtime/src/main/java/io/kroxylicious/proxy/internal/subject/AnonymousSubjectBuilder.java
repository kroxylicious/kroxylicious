/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.Subject;

public class AnonymousSubjectBuilder implements SubjectBuilder {
    public AnonymousSubjectBuilder() {
    }
    @Override
    public CompletionStage<Subject> buildSubject(Context context) {
        return CompletableFuture.completedStage(Subject.ANONYMOUS);
    }
}
