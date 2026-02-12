/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.inspection;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = MockSubjectBuilderService.Config.class)
public class MockSubjectBuilderService implements SaslSubjectBuilderService<MockSubjectBuilderService.Config> {
    @Override
    public void initialize(@NonNull Config config) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public SaslSubjectBuilder build() {
        throw new IllegalStateException("not implemented");
    }

    public record Config(String arbitraryProperty) {}

}
