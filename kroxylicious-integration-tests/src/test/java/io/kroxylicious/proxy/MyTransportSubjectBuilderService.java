/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.x500.X500Principal;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tls.ClientTlsContext;

@Plugin(configType = MyTransportSubjectBuilderService.Config.class)
public class MyTransportSubjectBuilderService implements TransportSubjectBuilderService<MyTransportSubjectBuilderService.Config> {

    public record Config(int delayMs, boolean completeSuccessfully) {
    }

    static class MyTransportSubjectBuilder implements TransportSubjectBuilder {

        private final int delayMs;
        private final boolean completeSuccessfully;

        public MyTransportSubjectBuilder(int delayMs, boolean completeSuccessfully) {
            this.delayMs = delayMs;
            this.completeSuccessfully = completeSuccessfully;
        }

        @Override
        public CompletionStage<Subject> buildTransportSubject(Context context) {
            return context.clientTlsContext()
                    .flatMap(ClientTlsContext::clientCertificate)
                    .map(tls -> delayed(new Subject(new User(tls.getSubjectX500Principal()
                            .getName(X500Principal.RFC1779,
                                    Map.of("1.2.840.113549.1.9.1", "emailAddress"))))))
                    .orElse(delayed(Subject.anonymous()));
        }

        CompletionStage<Subject> delayed(Subject subject) {
            if (delayMs == 0) {
                if (completeSuccessfully) {
                    return CompletableFuture.completedStage(subject);
                }
                else {
                    return CompletableFuture.failedStage(new RuntimeException("Oops"));
                }
            }
            else {
                var fut = new CompletableFuture<Subject>();
                new Thread(() -> {
                    try {
                        Thread.sleep(delayMs);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (completeSuccessfully) {
                        fut.complete(subject);
                    }
                    else {
                        fut.completeExceptionally(new RuntimeException("Oops"));
                    }
                }).start();
                return fut;
            }
        }
    }

    private MyTransportSubjectBuilder builder;

    @Override
    public void initialize(Config config) {
        this.builder = new MyTransportSubjectBuilder(config.delayMs, config.completeSuccessfully);
    }

    @Override
    public TransportSubjectBuilder build() {
        return this.builder;
    }
}
