/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = AclAuthorizerConfig.class)
public class AclAuthorizerService implements AuthorizerService<AclAuthorizerConfig> {

    private AclAuthorizerConfig config;

    @Override
    public void initialize(AclAuthorizerConfig config) {
        this.config = config;
    }

    @NonNull
    @Override
    public Authorizer build() throws IllegalStateException {
        var fileName = config.aclFile();
        // TODO parse the file!
        return new AclAuthorizer();
    }
}
