/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * API for defining plugins within configurtion.
 *
 * <h2>Terminology</h2>
 * <p>A <em>plugin interface</em> is a Java {@code interface} (or possibly a {@code class)} that can be used
 * in the implementation of a component of the proxy.
 * {@link io.kroxylicious.proxy.filter.FilterFactory} is one example of a plugin interface.</p>
 *
 * <p>A <em>plugin implementation</em> provides a concrete behaviour by implementing the plugin interface
 * (or extending the class).
 * There will usually be more than one plugin implementation for a given plugin interface.
 * Plugin implementations can be referenced in a configuration file by their
 * fully qualified class name, or by their unqualified class name if that is unambiguous.
 * Such references are known as <em>plugin implementation names</em>.
 * Any class that implements {@link io.kroxylicious.proxy.filter.FilterFactory} is an example of a
 * plugin implementation.
 *
 * <p>Plugin implementations often require some configuration to be provided in the configuration.
 * The configuration is represented in Java using a "config record", or "config class".
 * Different plugin implementations will generally use different config records.</p>
 *
 * <h2>Using a plugin implementation</h2>
 *
 * <p>The author of a {@code FooFilter} that wants to use a {@code HttpGetter} to make an HTTP GET request,
 * but doesn't want to depend directly on any particular HTTP client. {@code HttpGetter} is the plugin interface.</p>
 *
 * <pre>{@code
 * interface HttpGetter<C> {
 *     void configure(C config);
 *     String get(String url);
 * }
 * }</pre>
 *
 * <p>Note that this plugin interface doesn't know the concrete type of the configuration that an implementation requires.
 * In this case it is using a type parameter {@code C} to represent that.</p>
 *
 * <p>The config record for {@code FooFilter} would express its dependence on a {@code HttpGetter}, and also
 * provide a property to hold the {@code HttpGetter} implementation's own configuration, like this</p>
 *
 * <pre><code>
 * record FooFilterConfig(
 *   {@link io.kroxylicious.proxy.plugin.PluginImplName @PluginImplName}(HttpGetter.class)
 *   String httpGetterPluginImplName,
 *   {@link io.kroxylicious.proxy.plugin.PluginImplConfig @PluginImplConfig}(implNameProperty="httpGetterPluginImplName")
 *   Object httpGetterConfig
 * ) { }
 * </code></pre>
 *
 * <p>The {@link io.kroxylicious.proxy.plugin.PluginImplConfig#implNameProperty()} names the property of the config object that holds the
 * plugin implementation name. In practice the author of {@code FooFilter} might want to chose config property names which are intuitive to someone
 * writing a configuration file, such as {@code httpImpl} and {@code httpConfig}.</p>
 *
 * <p>The {@code FooFilter} author can then get an instance of the plugin implementation configured by the user using the
 * {@link io.kroxylicious.proxy.filter.FilterFactoryContext}, like this:</p>
 * <pre>{@code
 * class FooFilterFactory implements FilterFactory<FooFilterConfig, Void> {
 *     Void initialize(FilterFactoryContext context, FooFilterConfig config) {
 *       // get the configured HttpGetter
 *       this.httpGetter = context.pluginInstance(HttpGetter.class, config.httpGetterPluginImplName());
 *       // initialize it
 *       this.httpGetter.configure(config.httpGetterConfig());
 *       return null;
 *     }
 *     Filter createFilter(FilterFactoryContext context, Void v) {
 *         return new Filter() {
 *           // use the httpGetter
 *         };
 *     }
 * }
 * }</pre>
 *
 * <h2>Implementing a plugin</h2>
 *
 * <p>Someone can write an implementation of {@code HttpGetter} using Netty. They need to annotate their
 * implementation with {@link io.kroxylicious.proxy.plugin.Plugin @Plugin} to indicate
 * the type of configuration it uses.</p>
 *
 * <pre><code>
 * {@link io.kroxylicious.proxy.plugin.Plugin @Plugin}(configType=NettyConfig.class)
 * class NettyHttpGetter implements HttpGetter&lt;NettyConfig&gt; {
 *     // ...
 * }
 * </code></pre>
 *
 * <p>and provide a record for that config:</p>
 *
 * <pre><code>
 * record NettyConfig(String trustStore) { }
 * </code></pre>
 */
package io.kroxylicious.proxy.plugin;
