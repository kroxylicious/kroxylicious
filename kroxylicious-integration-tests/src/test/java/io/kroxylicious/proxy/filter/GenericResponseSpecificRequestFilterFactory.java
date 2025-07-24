/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = Void.class)
public class GenericResponseSpecificRequestFilterFactory implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) {
        return null;
    }

    @NonNull
    @Override
    public GenericResponseSpecificRequestFilter createFilter(FilterFactoryContext context, Void configuration) {
        return new GenericResponseSpecificRequestFilter();
    }

}
