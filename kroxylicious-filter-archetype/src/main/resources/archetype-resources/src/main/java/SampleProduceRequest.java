package ${package};

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import ${package}.config.SampleFilterConfig;

/**
 * A {@link FilterFactory} for {@link SampleProduceRequestFilter}.
 */
@Plugin(configType = SampleFilterConfig.class)
public class SampleProduceRequest implements FilterFactory<SampleFilterConfig, SampleFilterConfig> {

    @Override
    public SampleFilterConfig initialize(FilterFactoryContext context, SampleFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public SampleProduceRequestFilter createFilter(FilterFactoryContext context, SampleFilterConfig configuration) {
        return new SampleProduceRequestFilter(configuration);
    }

}
