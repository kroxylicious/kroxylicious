/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = TlsTestFilter.TlsTestFilterConfig.class)
public class TlsTestFilterFactory
        implements FilterFactory<TlsTestFilter.TlsTestFilterConfig, TlsTestFilterFactory.ContextCapture> {

    @Override
    public TlsTestFilterFactory.ContextCapture initialize(FilterFactoryContext context,
                                                          TlsTestFilter.TlsTestFilterConfig config) {
        return new ContextCapture(context, Plugins.requireConfig(this, config));
    }

    @NonNull
    @Override
    public TlsTestFilter createFilter(FilterFactoryContext context,
                                      TlsTestFilterFactory.ContextCapture configuration) {
        return new TlsTestFilter(configuration.config, configuration.initContext, context);
    }

    public record ContextCapture(FilterFactoryContext initContext, TlsTestFilter.TlsTestFilterConfig config) {}

}
