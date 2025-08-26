/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ConfigurationSerializationTest {
    public static final String NON_EXISTENT_PATH = "/tmp/" + UUID.randomUUID();
    ObjectMapper mapper = new ObjectMapper();

    public static Stream<Arguments> invalidJson() throws IOException {
        Path tempDir = Files.createTempDirectory(UUID.randomUUID().toString());
        return Stream.of(argumentSet("empty", "{}", MismatchedInputException.class, "Missing required creator property 'entraIdentity'"),
                argumentSet("oauthEndpoint not string",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": [],
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Cannot deserialize value of type `java.net.URI` from Array value"),
                argumentSet("oauthEndpoint not uri",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "bogus non uri",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        InvalidFormatException.class,
                        "Cannot deserialize value of type `java.net.URI` from String \"bogus non uri\""),
                argumentSet("scope not string",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    },
                                    "scope": []
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Cannot deserialize value of type `java.net.URI` from Array value"),
                argumentSet("scope not uri",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    },
                                    "scope": "bogus not uri"
                                  }
                                }
                                """,
                        InvalidFormatException.class,
                        "Cannot deserialize value of type `java.net.URI` from String \"bogus not uri\""),
                argumentSet("tenantId missing",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Missing required creator property 'tenantId'"),
                argumentSet("clientId missing",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Missing required creator property 'clientId'"),
                argumentSet("clientSecret missing",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Missing required creator property 'clientSecret'"),
                argumentSet("clientSecret file doesn't exist",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "passwordFile": "%s"
                                    }
                                  }
                                }
                                """.formatted(NON_EXISTENT_PATH),
                        ValueInstantiationException.class,
                        "Exception reading " + NON_EXISTENT_PATH),
                argumentSet("clientId file doesn't exist",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "passwordFile": "%s"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """.formatted(NON_EXISTENT_PATH),
                        ValueInstantiationException.class,
                        "Exception reading " + NON_EXISTENT_PATH),
                argumentSet("clientId file not a file",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "passwordFile": "%s"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """.formatted(tempDir),
                        ValueInstantiationException.class,
                        "Exception reading " + tempDir),
                argumentSet("keyVaultBaseUrl missing",
                        """
                                {
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Missing required creator property 'keyVaultBaseUrl'"),
                argumentSet("keyVaultBaseUrl not url",
                        """
                                {
                                  "keyVaultBaseUrl": "banabnan wdw dca",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        InvalidFormatException.class,
                        "Cannot deserialize value of type `java.net.URI` from String \"banabnan wdw dca\""),
                argumentSet("keyVaultBaseUrl not string",
                        """
                                {
                                  "keyVaultBaseUrl": [],
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "password": "def"
                                    }
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Cannot deserialize value of type `java.net.URI` from Array value"),
                argumentSet("entraIdentity not object",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": []
                                }
                                """, MismatchedInputException.class,
                        "Cannot deserialize value of type `io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig` from Array value "),
                argumentSet("clientSecret file not a file",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": {
                                    "oauthEndpoint": "http://oauth",
                                    "tenantId": "123",
                                    "clientId": {
                                      "password": "abc"
                                    },
                                    "clientSecret": {
                                      "passwordFile": "%s"
                                    }
                                  }
                                }
                                """.formatted(tempDir),
                        ValueInstantiationException.class,
                        "Exception reading " + tempDir));
    }

    @MethodSource
    @ParameterizedTest
    void invalidJson(String json, Class<? extends Exception> expectedType, String expectedMessage) {
        assertThatThrownBy(() -> mapper.readValue(json, AzureKeyVaultConfig.class)).isInstanceOf(expectedType).hasMessageContaining(expectedMessage);
    }

    @Test
    void validMinimalJson() throws IOException {
        String json = """
                {
                  "keyVaultBaseUrl": "http://my.vault",
                  "entraIdentity": {
                    "tenantId": "123",
                    "clientId": {
                      "password": "abc"
                    },
                    "clientSecret": {
                      "password": "def"
                    }
                  }
                }
                """;
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        assertThat(config).isEqualTo(
                new AzureKeyVaultConfig(new EntraIdentityConfig(null, "123", new InlinePassword("abc"), new InlinePassword("def"), null, null),
                        URI.create("http://my.vault"), null));
    }

    @Test
    void minimumJsonFidelity() throws IOException {
        Path clientId = Files.createTempFile("clientId", ".txt");
        Path clientSecret = Files.createTempFile("clientSecret", ".txt");
        Files.writeString(clientId, "abc");
        Files.writeString(clientSecret, "def");
        String json = """
                {
                  "entraIdentity": {
                        "tenantId": "123",
                        "clientId": {
                          "password": "abc"
                        },
                        "clientSecret": {
                          "password": "def"
                        }
                  },
                  "keyVaultBaseUrl": "http://my.vault"
                }
                """.formatted(clientId, clientSecret);
        String normalized = mapper.writeValueAsString(mapper.readValue(json, JsonNode.class));
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        String actual = mapper.writeValueAsString(config);
        assertThat(actual).isEqualTo(normalized);
    }

    @Test
    void validComprehensiveJson() throws IOException {
        Path clientId = Files.createTempFile("clientId", ".txt");
        Path clientSecret = Files.createTempFile("clientSecret", ".txt");
        Files.writeString(clientId, "abc");
        Files.writeString(clientSecret, "def");
        String json = """
                   {
                  "keyVaultBaseUrl": "http://my.vault",
                  "entraIdentity": {
                    "oauthEndpoint": "http://localhost:8080",
                    "tenantId": "123",
                    "clientId": {
                      "password": "abc"
                    },
                    "clientSecret": {
                      "password": "def"
                    },
                    "scope": "http://scope/.default"
                  }
                }
                """.formatted(clientId, clientSecret);
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        assertThat(config).isEqualTo(
                new AzureKeyVaultConfig(
                        new EntraIdentityConfig(URI.create("http://localhost:8080"), "123", new InlinePassword("abc"), new InlinePassword("def"),
                                URI.create("http://scope/.default"),
                                null),
                        URI.create("http://my.vault"), null));
    }

    @Test
    void comprehensiveJsonFidelity() throws IOException {
        Path clientId = Files.createTempFile("clientId", ".txt");
        Path clientSecret = Files.createTempFile("clientSecret", ".txt");
        Files.writeString(clientId, "abc");
        Files.writeString(clientSecret, "def");
        String json = """
                   {
                  "entraIdentity": {
                    "oauthEndpoint": "http://localhost:8080",
                    "tenantId": "123",
                    "clientId": {
                      "password": "abc"
                    },
                    "clientSecret": {
                      "password": "def"
                    },
                    "scope": "http://scope/.default"
                   },
                  "keyVaultBaseUrl": "http://my.vault"
                }
                """.formatted(clientId, clientSecret);
        String normalized = mapper.writeValueAsString(mapper.readValue(json, JsonNode.class));
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        String actual = mapper.writeValueAsString(config);
        assertThat(actual).isEqualTo(normalized);
    }
}
