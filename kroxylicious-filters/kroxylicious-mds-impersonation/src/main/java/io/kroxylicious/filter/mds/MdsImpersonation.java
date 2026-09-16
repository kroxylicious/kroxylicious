/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.http.HttpClient;
import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.NonNull;

/** Exchanges a verified mTLS client identity for an MDS impersonation token. */
@Plugin(configType = MdsImpersonationConfig.class)
public class MdsImpersonation implements FilterFactory<MdsImpersonationConfig, MdsImpersonation.Context> {
    /** Constructor for ServiceLoader. */
    public MdsImpersonation() {
    }

    /** Shared factory resources; contains no per-user token cache. */
    public static final class Context {
        private final MdsImpersonationConfig config;
        private final MdsClient client;
        private final ExecutorService httpExecutor;

        private Context(MdsImpersonationConfig config, MdsClient client, ExecutorService httpExecutor) {
            this.config = config;
            this.client = client;
            this.httpExecutor = httpExecutor;
        }
    }

    @Override
    public Context initialize(FilterFactoryContext context, MdsImpersonationConfig config) {
        var required = Plugins.requireConfig(this, config);
        var httpExecutor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(256),
                Thread.ofPlatform().daemon().name("mds-http-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
        try {
            // Abort rather than running HTTP work on a caller's filter dispatch thread.
            var builder = HttpClient.newBuilder().executor(httpExecutor).connectTimeout(required.requestTimeout())
                    .followRedirects(HttpClient.Redirect.NEVER);
            var http = new TlsHttpClientConfigurator(required.mdsTls()).apply(builder).build();
            return new Context(required, new MdsClient(http, required), httpExecutor);
        }
        catch (RuntimeException e) {
            httpExecutor.shutdownNow();
            throw e;
        }
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, @NonNull Context initializationData) {
        return new MdsImpersonationFilter(initializationData.client::impersonate,
                initializationData.config.expiryMargin(), context.filterDispatchExecutor(), Clock.systemUTC());
    }

    @Override
    public void close(@NonNull Context initializationData) {
        try {
            initializationData.client.close();
        }
        finally {
            initializationData.httpExecutor.shutdownNow();
        }
    }
}
