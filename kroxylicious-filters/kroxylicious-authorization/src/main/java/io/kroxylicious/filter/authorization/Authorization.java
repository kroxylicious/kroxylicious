/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.filter.authorization.subject.ClientSubjectBuilder;
import io.kroxylicious.filter.authorization.subject.ClientSubjectBuilderService;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

public class Authorization implements FilterFactory<AuthorizationConfig, Authorization.InitializationContext> {

    record InitializationContext(Authorizer authorizer, ClientSubjectBuilder subjectBuilder) {
    }

    @Override
    public InitializationContext initialize(FilterFactoryContext context, AuthorizationConfig configuration) throws PluginConfigurationException {
        var authorizerService = context.pluginInstance(AuthorizerService.class, configuration.authorizer());
        authorizerService.initialize(configuration.authorizerConfig());
        var authorizer = authorizerService.build();

        var subjectBuilderService = context.pluginInstance(ClientSubjectBuilderService.class, configuration.subjectBuilder());
        subjectBuilderService.initialize(configuration.subjectBuilderConfig());
        var subjectBuilder = subjectBuilderService.build();

        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, InitializationContext initializationData) {
        return new AuthorizationFilter(initializationData.authorizer(), initializationData.subjectBuilder(), null);
    }
}
