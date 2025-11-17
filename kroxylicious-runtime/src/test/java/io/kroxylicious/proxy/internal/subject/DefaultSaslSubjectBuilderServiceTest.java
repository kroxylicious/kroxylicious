/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultSaslSubjectBuilderServiceTest {

    private static YAMLMapper mapper = new YAMLMapper();

    private static void sasl(DefaultSaslSubjectBuilderService.Config builderConfig,
                             String authorizedId,
                             @Nullable Subject expectedSubject) {
        var service = new DefaultSaslSubjectBuilderService();
        service.initialize(builderConfig);
        var builder = service.build();
        var subject = builder.buildSaslSubject(new SaslSubjectBuilder.Context() {
            @Override
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.of(new ClientTlsContext() {
                    @Override
                    public X509Certificate proxyServerCertificate() {
                        throw new AssertionError();
                    }

                    @Override
                    public Optional<X509Certificate> clientCertificate() {
                        throw new AssertionError();
                    }
                });
            }

            @Override
            public ClientSaslContext clientSaslContext() {
                return new ClientSaslContext() {
                    @Override
                    public String mechanismName() {
                        return "WHATEVS";
                    }

                    @Override
                    public String authorizationId() {
                        return authorizedId;
                    }
                };
            }
        });

        Assertions.assertThat(subject).isCompleted();
        var fut = subject.toCompletableFuture();
        Assertions.assertThat(fut.join())
                .isEqualTo(expectedSubject);
    }

    static List<Arguments> invalidShouldThrowDuringDeserialization() {
        return List.of(
                Arguments.argumentSet(
                        "unknown from",
                        """
                                addPrincipals:
                                  - from: thisIsNotKnown
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Unknown `from` 'thisIsNotKnown', supported values are: "
                                + "'saslAuthorizedId'."),
                Arguments.argumentSet(
                        "unknown map",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - thisIsNotKnown: ""
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Exactly one of `replaceMatch` and `else` are required."),
                Arguments.argumentSet(
                        "unknown else",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /.*/foo/
                                      - else: thisIsNotKnown
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "`else` can only take the value 'identity' or 'anonymous'."),
                Arguments.argumentSet(
                        "multiple else",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /.*/foo/
                                      - else: identity
                                      - else: identity
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "An `else` mapping may only occur as the last element of `map`."),
                Arguments.argumentSet(
                        "else not the last mapping",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - else: identity
                                      - replaceMatch: /.*/foo/
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "An `else` mapping may only occur as the last element of `map`."),
                Arguments.argumentSet(
                        "map not one of",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - else: identity
                                        replaceMatch: /.*/foo/
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "`replaceMatch` and `else` are mutually exclusive."),
                Arguments.argumentSet(
                        "unknown principal factory",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    principalFactory: this.is.not.Known
                                """,
                        "`principalFactory` 'this.is.not.Known' not found."),
                Arguments.argumentSet(
                        "empty replaceMatch",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: ""
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule: rule is empty, but it should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing."),
                Arguments.argumentSet(
                        "replaceMatch pattern not terminated #1",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 0: '/' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs once, at the start of the string. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch pattern not terminated #2",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /foo
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 3: '/foo' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs once, at the start of the string. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch replacement not terminated #1",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /foo/
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 4: '/foo/' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs twice. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch replacement not terminated #2",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /foo/bar
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 7: '/foo/bar' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                                + "where `c` is a separator character of your choosing. "
                                + "You seem to be using '/' as the separator character, but it only occurs twice. "
                                + "(Hint: The rule format is not the same as Kafka's)."),
                Arguments.argumentSet(
                        "replaceMatch illegal flags",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /foo/bar/badflags
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 9: The given flags, 'badflags', are not valid. The flags may be empty or 'L' or 'U'."),
                Arguments.argumentSet(
                        "replaceMatch multiple legal flags",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /foo/bar/LU
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 9: The given flags, 'LU', are not valid. The flags may be empty or 'L' or 'U'."),
                Arguments.argumentSet(
                        "replaceMatch invalid regex",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /.***/bar/L
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        "Invalid mapping rule at index 1: The pattern part of the rule, '.***', is not a valid regular expression in RE2 format: "
                                + "invalid nested repetition operator."));
    }

    @ParameterizedTest
    @MethodSource
    void invalidShouldThrowDuringDeserialization(String rule, String expectedExceptionMessage) {
        assertThatThrownBy(() -> {
            mapper.readValue(rule, DefaultSaslSubjectBuilderService.Config.class);
        })
                .isInstanceOf(JsonProcessingException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedExceptionMessage);
    }

    static List<Arguments> rulesShouldWorkWithSasl() {
        return List.of(
                Arguments.argumentSet("sasl",
                        "my-sasl-name",
                        """
                                addPrincipals:
                                  - from: saslAuthorizedId
                                    map:
                                      - replaceMatch: /(.*?)-sasl-(.*?)/$1-$2/
                                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                                """,
                        new Subject(new User("my-name"))));
    }

    @ParameterizedTest
    @MethodSource
    void rulesShouldWorkWithSasl(String authzId, String rule, Subject expectedName)
            throws JsonProcessingException {
        DefaultSaslSubjectBuilderService.Config builderConfig = mapper.readValue(rule, DefaultSaslSubjectBuilderService.Config.class);
        sasl(builderConfig,
                authzId,
                expectedName);
    }

    @Test
    void firstMatchingMapShouldWin()
            throws JsonProcessingException {

        DefaultSaslSubjectBuilderService.Config builderConfig = mapper.readValue("""
                addPrincipals:
                  - from: saslAuthorizedId
                    map:
                      - replaceMatch: /foo-(.*?)/$1/
                      - replaceMatch: /(.+)/$1/U
                      - else: anonymous
                    principalFactory: io.kroxylicious.proxy.internal.subject.UserFactory
                """, DefaultSaslSubjectBuilderService.Config.class);

        sasl(builderConfig,
                "foo-apple",
                new Subject(new User("apple")));
        sasl(builderConfig,
                "bar-pear",
                new Subject(new User("BAR-PEAR")));
        sasl(builderConfig,
                "",
                new Subject());
    }

}
