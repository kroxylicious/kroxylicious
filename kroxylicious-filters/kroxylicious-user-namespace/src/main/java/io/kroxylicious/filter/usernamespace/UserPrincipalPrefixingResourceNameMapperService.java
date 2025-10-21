/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = Void.class)
public class UserPrincipalPrefixingResourceNameMapperService implements ResourceNameMapperService<Void> {
    @Override
    public void initialize(Void config) {
        // intentionally empty
    }

    @NonNull
    @Override
    public ResourceNameMapper build() {
        return new UserPrincipalPrefixingResourceNameMapper();
    }

}
