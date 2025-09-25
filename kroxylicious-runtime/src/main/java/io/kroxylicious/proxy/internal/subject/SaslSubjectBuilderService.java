/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

public class SaslSubjectBuilderService implements SubjectBuilderService<Void> {

    @Override
    public void initialize(Void config) {
    }

    @Override
    public SubjectBuilder build() {
        return new SaslSubjectBuilder();
    }

}
