package ${package};

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import ${package}.config.SampleFilterConfig;

@Plugin(configType = SampleFilterConfig.class)
public class SampleFetchResponse implements FilterFactory<SampleFilterConfig, SampleFilterConfig> {

    @Override
    public SampleFilterConfig initialize(FilterFactoryContext context, SampleFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public SampleFetchResponseFilter createFilter(FilterFactoryContext context, SampleFilterConfig configuration) {
        return new SampleFetchResponseFilter(configuration);
    }

}
