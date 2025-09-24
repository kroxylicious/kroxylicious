/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization.subject;

public class SaslSubjectBuilderService implements ClientSubjectBuilderService<Void> {

    @Override
    public void initialize(Void config) {
    }

    @Override
    public ClientSubjectBuilder build() {
        return new SaslSubjectBuilder();
    }

}
