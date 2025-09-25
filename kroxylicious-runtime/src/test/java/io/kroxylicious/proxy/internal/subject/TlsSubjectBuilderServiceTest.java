/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import javax.security.auth.x500.X500Principal;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

class TlsSubjectBuilderServiceTest {

    private static void extracted(String expect, X509Certificate cert, TlsSubjectBuilderService.Config config) {
        var service = new TlsSubjectBuilderService();
        service.initialize(config);
        SubjectBuilder builder = service.build();
        var subject = builder.buildSubject(new SubjectBuilder.Context() {
            @Override
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.of(new ClientTlsContext() {
                    @Override
                    public X509Certificate proxyServerCertificate() {
                        return null;
                    }

                    @Override
                    public Optional<X509Certificate> clientCertificate() {
                        return Optional.of(cert);
                    }
                });
            }

            @Override
            public Optional<ClientSaslContext> clientSaslContext() {
                return Optional.empty();
            }
        });

        Assertions.assertThat(subject).isCompleted();
        Assertions.assertThat(subject.toCompletableFuture().join())
                .extracting(Subject::principals, InstanceOfAssertFactories.set(Principal.class))
                .singleElement()
                .isInstanceOf(User.class)
                .extracting(Principal::name, InstanceOfAssertFactories.STRING)
                .isEqualTo(expect);
    }

    static List<Arguments> x500Subject() {
        return List.of(
                Arguments.argumentSet("DEFAULT", "CN=test,OU=testing", "DEFAULT", "CN=test,OU=testing"),
                Arguments.argumentSet("replace", "CN=test,OU=testing", "CN=(.*),OU=testing/X$1Y/", "XtestY"),
                Arguments.argumentSet("replace+lower", "CN=test,OU=testing", "CN=(.*),OU=testing/X$1Y/L", "xtesty"),
                Arguments.argumentSet("replace+upper", "CN=test,OU=testing", "CN=(.*),OU=testing/X$1Y/U", "XTESTY")
                // TODO a name that doesn't match
                // TODO The other ways of formatting the X500 name
        );
    }

    @ParameterizedTest
    @MethodSource
    void x500Subject(String x500Principal, String rule, String expect) {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(new X500Principal(x500Principal));

        extracted(expect, cert, new TlsSubjectBuilderService.Config(
                null,
                List.of(rule),
                null,
                null,
                null,
                null,
                null));
    }

    static List<Arguments> sanRfc822() {
        return List.of(
                Arguments.argumentSet("DEFAULT", "test@example.com", "DEFAULT", "test@example.com"),
                Arguments.argumentSet("replace", "test@example.com", "(.*)@example.com/X$1Y/", "XtestY"),
                Arguments.argumentSet("replace+lower", "test@example.com", "(.*)@example.com/X$1Y/L", "xtesty"),
                Arguments.argumentSet("replace+upper", "test@example.com", "(.*)@example.com/X$1Y/U", "XTESTY")
                // TODO a name that doesn't match
        );
    }

    @ParameterizedTest
    @MethodSource
    void sanRfc822(String rfc822, String rule, String expect) throws CertificateParsingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(1, rfc822)));

        extracted(expect, cert, new TlsSubjectBuilderService.Config(
                null,
                null,
                List.of(rule),
                null,
                null,
                null,
                null));
    }

    // TODO the other SANs

    // TODO multiple SANs => multiple users

    // TODO the invalid rules throw
    // TODO that no rules at all (all nulls) is an error

    // TODO the other SAN types don't result in names

}