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
import java.util.concurrent.CompletionException;

import javax.security.auth.x500.X500Principal;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultTransportSubjectBuilderServiceTest {

    private static YAMLMapper mapper = new YAMLMapper();

    private static void tls(DefaultTransportSubjectBuilderService.Config builderConfig,
                            X509Certificate cert,
                            @Nullable Subject expectedSubject,
                            @Nullable Exception expectedException) {
        if ((expectedSubject != null) == (expectedException != null)) {
            throw new IllegalArgumentException("These arguments are mutually exclusive.");
        }
        var service = new DefaultTransportSubjectBuilderService();
        service.initialize(builderConfig);
        TransportSubjectBuilder builder = service.build();
        var subject = builder.buildTransportSubject(new TransportSubjectBuilder.Context() {
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
        });

        Assertions.assertThat(subject).isCompleted();
        var fut = subject.toCompletableFuture();
        if (expectedSubject != null) {
            Assertions.assertThat(fut.join())
                    // .extracting(Subject::principals, InstanceOfAssertFactories.set(Principal.class))
                    // .singleElement()
                    // .isInstanceOf(User.class)
                    // .extracting(Principal::name, InstanceOfAssertFactories.STRING)
                    .isEqualTo(expectedSubject);
        }
        else {
            Assertions.assertThat(fut)
                    .isCompletedExceptionally();
            assertThatThrownBy(() -> fut.join()).isExactlyInstanceOf(CompletionException.class)
                    .cause()
                    .isExactlyInstanceOf(expectedException.getClass())
                    .hasMessage(expectedException.getMessage());
        }

    }

    static List<Arguments> invalidShouldThrowDuringDeserialization() {
        return List.of(
                Arguments.argumentSet(
                        "unknown from",
                        """
                                addPrincipals:
                                  - from: thisIsNotKnown
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Unknown `from` 'thisIsNotKnown', supported values are: "
                                + "'clientTlsSubject', 'clientTlsSanRfc822Name', "
                                + "'clientTlsSanDirName', 'clientTlsSanDnsName', 'clientTlsSanUri', "
                                + "'clientTlsSanIpAddress'."),
                Arguments.argumentSet(
                        "unknown map",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - thisIsNotKnown: ""
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Exactly one of `replaceMatch` and `else` are required."),
                Arguments.argumentSet(
                        "unknown else",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /.*/foo/
                                      - else: thisIsNotKnown
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "`else` can only take the value 'identity' or 'anonymous'."),
                Arguments.argumentSet(
                        "multiple else",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /.*/foo/
                                      - else: identity
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "An `else` mapping may only occur as the last element of `map`."),
                Arguments.argumentSet(
                        "else not the last mapping",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - else: identity
                                      - replaceMatch: /.*/foo/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "An `else` mapping may only occur as the last element of `map`."),
                Arguments.argumentSet(
                        "map not one of",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - else: identity
                                        replaceMatch: /.*/foo/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "`replaceMatch` and `else` are mutually exclusive."),
                Arguments.argumentSet(
                        "unknown principal factory",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    principalFactory: this.is.not.Known
                                """,
                        "`principalFactory` 'this.is.not.Known' not found."),
                Arguments.argumentSet(
                        "empty replaceMatch",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: ""
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule: rule is empty, but it should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing."),
                Arguments.argumentSet(
                        "replaceMatch pattern not terminated #1",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 0: '/' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs once, at the start of the string. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch pattern not terminated #2",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /foo
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 3: '/foo' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs once, at the start of the string. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch replacement not terminated #1",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /foo/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 4: '/foo/' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs twice. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch replacement not terminated #2",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /foo/bar
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 7: '/foo/bar' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs twice. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch illegal flags",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /foo/bar/badflags
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 9: The given flags, 'badflags', are not valid. The flags may be empty or 'L' or 'U'."),
                Arguments.argumentSet(
                        "replaceMatch multiple legal flags",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /foo/bar/LU
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 9: The given flags, 'LU', are not valid. The flags may be empty or 'L' or 'U'."),
                Arguments.argumentSet(
                        "replaceMatch invalid regex",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /.***/bar/L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        "Invalid mapping rule at index 1: The pattern part of the rule, '.***', is not a valid regular expression in RE2 format: "
                                + "invalid nested repetition operator."));
    }

    @ParameterizedTest
    @MethodSource
    void invalidShouldThrowDuringDeserialization(String rule, String expectedExceptionMessage) {
        assertThatThrownBy(() -> {
            mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        })
                .isInstanceOf(JsonProcessingException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedExceptionMessage);
    }

    static List<Arguments> rulesShouldWorkWithX500Subjects() {
        return List.of(
                Arguments.argumentSet("empty adders", "CN=test,OU=testing", """
                        addPrincipals: []
                        """,
                        new Subject(), null),
                Arguments.argumentSet("empty mappings", "CN=test,OU=testing", """
                        addPrincipals:
                          - from: clientTlsSubject
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                        """,
                        new Subject(new User("CN=test,OU=testing")), null),
                Arguments.argumentSet("no match,no else => anonymous", "CN=test,OU=nottesting", """
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=testing/X$1Y/
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                        """,
                        new Subject(), null),
                Arguments.argumentSet("no match+else identity", "CN=test,OU=nottesting", """
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=testing/X$1Y/
                              - else: identity
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                        """,
                        new Subject(new User("CN=test,OU=nottesting")), null),
                Arguments.argumentSet("no match+else anonymous", "CN=test,OU=nottesting", """
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=testing/X$1Y/
                              - else: anonymous
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                        """,
                        new Subject(), null),
                Arguments.argumentSet("multiple", "CN=test,OU=testing", """
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=(.*)/X$1Y/U
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=(.*)/X$2Y/L
                            principalFactory: io.kroxylicious.proxy.internal.subject.UnitFactory
                        """,
                        new Subject(new User("XTESTY"), new Unit("xtestingy")), null),
                Arguments.argumentSet("replace, no flags", "CN=test,OU=testing", """
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: /CN=(.*),OU=testing/X$1Y/
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                        """,
                        new Subject(new User("XtestY")), null),
                Arguments.argumentSet("replace+lowercase",
                        "CN=test,OU=testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /CN=(.*),OU=testing/X$1Y/L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("xtesty")), null),
                Arguments.argumentSet("replace+uppercase",
                        "CN=test,OU=testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSubject
                                    map:
                                      - replaceMatch: /CN=(.*),OU=testing/X$1Y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTESTY")), null)
        // TODO The other ways of formatting the X500 name (i.e. not CANONICAL etc)
        );
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithX500Subjects(String x500Principal, String rule, Subject expectedSubject, Exception expectedException) throws JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(new X500Principal(x500Principal));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);

        tls(builderConfig,
                cert,
                expectedSubject,
                expectedException);
    }

    static List<Arguments> rulesShouldWorkWithSanRfc822Names() {
        return List.of(
                Arguments.argumentSet("empty adders",
                        "test@testing.example.com", """
                                addPrincipals: []
                                """,
                        new Subject(), null),
                Arguments.argumentSet("empty mappings",
                        "test@testing.example.com", """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("test@testing.example.com")), null),
                Arguments.argumentSet("no match,no else => anonymous",
                        "test@nottesting.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("no match+else identity",
                        "test@nottesting.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("test@nottesting.example.org")), null),
                Arguments.argumentSet("no match+else anonymous",
                        "test@testing.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/
                                      - else: anonymous
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("multiple",
                        "test@testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$2Y/L
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UnitFactory
                                """,
                        new Subject(new User("XTESTY"), new Unit("xtestingy")), null),
                Arguments.argumentSet("replace, no flags",
                        "test@testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XtestY")), null),
                Arguments.argumentSet("replace+lowercase",
                        "test@testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("xtesty")), null),
                Arguments.argumentSet("replace+uppercase",
                        "test@testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanRfc822Name
                                    map:
                                      - replaceMatch: /(.*)@(.*)[.]example[.]com/X$1Y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTESTY")), null));
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSanRfc822Names(String rfc822, String rule, Subject expectedName, Exception expectedException)
            throws CertificateParsingException, JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(TlsCertificateExtractor.Asn1SanNameType.RFC822.asn1Value, rfc822)));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        tls(builderConfig,
                cert,
                expectedName,
                expectedException);
    }

    static List<Arguments> rulesShouldWorkWithSanDnsNames() {
        return List.of(
                Arguments.argumentSet("empty adders",
                        "test.testing.example.com",
                        """
                                addPrincipals: []
                                """,
                        new Subject(), null),
                Arguments.argumentSet("empty mappings",
                        "test.testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("test.testing.example.com")), null),
                Arguments.argumentSet("no match,no else => anonymous",
                        "test.testing.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("no match+else identity",
                        "test.testing.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("test.testing.example.org")), null),
                Arguments.argumentSet("no match+else anonymous",
                        "test.testing.example.org",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/
                                      - else: anonymous
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("multiple",
                        "test.testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/x$1y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$2Y/L
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UnitFactory
                                """,
                        new Subject(new User("XTESTY"), new Unit("xtestingy")), null),
                Arguments.argumentSet("replace, no flags",
                        "test.testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XtestY")), null),
                Arguments.argumentSet("replace+lowercase",
                        "test.testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("xtesty")), null),
                Arguments.argumentSet("replace+uppercase",
                        "test.testing.example.com",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDnsName
                                    map:
                                      - replaceMatch: /(.*)[.](.*)[.]example[.]com/X$1Y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTESTY")), null));
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSanDnsNames(String rfc822, String rule, Subject expectedName, Exception expectedException)
            throws CertificateParsingException, JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(TlsCertificateExtractor.Asn1SanNameType.DNS.asn1Value, rfc822)));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        tls(builderConfig,
                cert,
                expectedName,
                expectedException);
    }

    static List<Arguments> rulesShouldWorkWithSanDirNames() {
        return List.of(
                Arguments.argumentSet("empty adders",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals: []
                                """,
                        new Subject(), null),
                Arguments.argumentSet("empty mappings",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("CN=Test,OU=Testing,O=Example Corp,C=GB")), null),
                Arguments.argumentSet("no match,no else => anonymous",
                        "CN=Test,OU=Testing,O=Example Corp,C=US",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("no match+else identity",
                        "CN=Test,OU=Testing,O=Example Corp,C=US",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("CN=Test,OU=Testing,O=Example Corp,C=US")), null),
                Arguments.argumentSet("no match+else anonymous",
                        "CN=Test,OU=Testing,O=Example Corp,C=US",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/
                                      - else: anonymous
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("multiple",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/x$1y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$2Y/L
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UnitFactory
                                """,
                        new Subject(new User("XTESTY"), new Unit("xtestingy")), null),
                Arguments.argumentSet("replace, no flags",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTestY")), null),
                Arguments.argumentSet("replace+lowercase",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("xtesty")), null),
                Arguments.argumentSet("replace+uppercase",
                        "CN=Test,OU=Testing,O=Example Corp,C=GB",
                        """
                                addPrincipals:
                                  - from: clientTlsSanDirName
                                    map:
                                      - replaceMatch: /CN=([^,]*),OU=([^,]*),.*,C=GB/X$1Y/U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTESTY")), null));
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSanDirNames(String rfc822, String rule, Subject expectedName, Exception expectedException)
            throws CertificateParsingException, JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(TlsCertificateExtractor.Asn1SanNameType.DIR_NAME.asn1Value, rfc822)));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        tls(builderConfig,
                cert,
                expectedName,
                expectedException);
    }

    static List<Arguments> rulesShouldWorkWithSanUri() {
        return List.of(
                Arguments.argumentSet("empty adders",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals: []
                                """,
                        new Subject(), null),
                Arguments.argumentSet("empty mappings",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("scheme://test.example.com/testing")), null),
                Arguments.argumentSet("no match,no else => anonymous",
                        "scheme://test.example.org/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("no match+else identity",
                        "scheme://test.example.org/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("scheme://test.example.org/testing")), null),
                Arguments.argumentSet("no match+else anonymous",
                        "scheme://test.example.org/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=
                                      - else: anonymous
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(), null),
                Arguments.argumentSet("multiple",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$2Y=L
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UnitFactory
                                """,
                        new Subject(new User("XTESTY"), new Unit("xtestingy")), null),
                Arguments.argumentSet("replace, no flags",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XtestY")), null),
                Arguments.argumentSet("replace+lowercase",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=L
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("xtesty")), null),
                Arguments.argumentSet("replace+uppercase",
                        "scheme://test.example.com/testing",
                        """
                                addPrincipals:
                                  - from: clientTlsSanUri
                                    map:
                                      - replaceMatch: =scheme://(.*)[.]example[.]com/(.*)=X$1Y=U
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("XTESTY")), null));
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSanUri(String rfc822, String rule, Subject expectedName, Exception expectedException)
            throws CertificateParsingException, JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(TlsCertificateExtractor.Asn1SanNameType.URI.asn1Value, rfc822)));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        tls(builderConfig,
                cert,
                expectedName,
                expectedException);
    }

    static List<Arguments> rulesShouldWorkWithSanIpAddress() {
        return List.of(
                Arguments.argumentSet("ipv4",
                        "123.123.123.80",
                        """
                                addPrincipals:
                                  - from: clientTlsSanIpAddress
                                    map:
                                      - replaceMatch: /123.123.123.([0-9]+)/foo-$1/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("foo-80")), null),
                Arguments.argumentSet("ipv6",
                        "a1:a2:a3:a4:a5:a6:a7:a8",
                        """
                                addPrincipals:
                                  - from: clientTlsSanIpAddress
                                    map:
                                      - replaceMatch: /[0-9a-f]{2}:(.*):[0-9a-f]{2}:[0-9a-f]{2}/X$1Y/
                                    principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                                """,
                        new Subject(new User("Xa2:a3:a4:a5:a6Y")), null)

        );
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSanIpAddress(String rfc822, String rule, Subject expectedName, Exception expectedException)
            throws CertificateParsingException, JsonProcessingException {
        var cert = Mockito.mock(X509Certificate.class);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(List.of(List.of(TlsCertificateExtractor.Asn1SanNameType.IP_ADDRESS.asn1Value, rfc822)));

        DefaultTransportSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultTransportSubjectBuilderService.Config.class);
        tls(builderConfig,
                cert,
                expectedName,
                expectedException);
    }

    // TODO the other SAN types don't result in names

}
