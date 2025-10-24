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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.provider.azure.config.auth.ManagedIdentityConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ConfigurationSerializationTest {
    public static final String NON_EXISTENT_PATH = "/tmp/" + UUID.randomUUID();
    ObjectMapper mapper = new ObjectMapper();

    public static Stream<Arguments> invalidJson() throws IOException {
        Path tempDir = Files.createTempDirectory(UUID.randomUUID().toString());
        return Stream.of(
                argumentSet("empty", "{}", MismatchedInputException.class, "Missing required creator property 'keyVaultName'"),
                argumentSet("entraIdentity oauthEndpoint not string",
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
                argumentSet("entraIdentity oauthEndpoint not uri",
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
                argumentSet("entraIdentity scope not string",
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
                argumentSet("entraIdentity scope not uri",
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
                argumentSet("entraIdentity tenantId missing",
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
                argumentSet("entraIdentity clientId missing",
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
                argumentSet("entraIdentity clientSecret missing",
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
                argumentSet("entraIdentity clientSecret file doesn't exist",
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
                argumentSet("entraIdentity clientId file doesn't exist",
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
                argumentSet("entraIdentity clientId file not a file",
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
                argumentSet("keyVaultName missing",
                        """
                                {
                                  "keyVaultHost": "my.vault.com",
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
                        "Missing required creator property 'keyVaultName'"),
                argumentSet("keyVaultName not string",
                        """
                                {
                                  "keyVaultName": [],
                                  "keyVaultHost": "my.vault.com",
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
                        "Cannot deserialize value of type `java.lang.String` from Array value"),
                argumentSet("keyVaultName null",
                        """
                                {
                                  "keyVaultName": null,
                                  "keyVaultHost": "my.vault.com",
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
                        ValueInstantiationException.class,
                        "Cannot construct instance of `io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig`"),
                argumentSet("keyVaultHost missing",
                        """
                                {
                                  "keyVaultName": "kv-name",
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
                        "Missing required creator property 'keyVaultHost'"),
                argumentSet("keyVaultHost not string",
                        """
                                {
                                  "keyVaultName": "kvname",
                                  "keyVaultHost": [],
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
                        "Cannot deserialize value of type `java.lang.String` from Array value"),
                argumentSet("keyVaultName null",
                        """
                                {
                                  "keyVaultName": "abc",
                                  "keyVaultHost": null,
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
                        ValueInstantiationException.class,
                        "Cannot construct instance of `io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig`"),
                argumentSet("entraIdentity not object",
                        """
                                {
                                  "keyVaultBaseUrl": "http://my.vault",
                                  "entraIdentity": []
                                }
                                """, MismatchedInputException.class,
                        "Cannot deserialize value of type `io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig` from Array value "),
                argumentSet("entraIdentity clientSecret file not a file",
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
                        "Exception reading " + tempDir),
                argumentSet("managedIdentity targetResource missing",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "managedIdentity": {
                                    "identityServiceEndpoint": "http://localhost:8080"
                                  }
                                }
                                """,
                        MismatchedInputException.class,
                        "Missing required creator property 'targetResource'"),
                argumentSet("managedIdentity identityServiceEndpoint not valid uri",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "managedIdentity": {
                                    "targetResource": "https://example.com/",
                                    "identityServiceEndpoint": "bogus not uri"
                                  }
                                }
                                """,
                        InvalidFormatException.class,
                        "Cannot deserialize value of type `java.net.URI` from String \"bogus not uri\""));
    }

    @MethodSource
    @ParameterizedTest
    void invalidJson(String json, Class<? extends Exception> expectedType, String expectedMessage) {
        assertThatThrownBy(() -> mapper.readValue(json, AzureKeyVaultConfig.class)).isInstanceOf(expectedType).hasMessageContaining(expectedMessage);
    }

    public static Stream<Arguments> validJson() {
        return Stream.of(
                argumentSet("valid minimal json with entra identity authentication",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
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
                                """,
                        new AzureKeyVaultConfig(new EntraIdentityConfig(null, "123", new InlinePassword("abc"), new InlinePassword("def"), null, null), null,
                                "my-key-vault", "vault.azure.net", null, null, null)),
                argumentSet("valid minimal json with managed identity authentication",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "managedIdentity": {
                                    "targetResource": "https://example.com/"
                                  }
                                }
                                """,
                        new AzureKeyVaultConfig(null, new ManagedIdentityConfig("https://example.com/", null), "my-key-vault", "vault.azure.net", null, null,
                                null)),
                argumentSet("valid comprehensive json with entra identity authentication",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "keyVaultScheme": "https",
                                  "keyVaultPort": 8080,
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
                                """,
                        new AzureKeyVaultConfig(new EntraIdentityConfig(URI.create("http://localhost:8080"), "123", new InlinePassword("abc"), new InlinePassword("def"),
                                URI.create("http://scope/.default"), null), null, "my-key-vault", "vault.azure.net", "https", 8080, null)),
                argumentSet("valid comprehensive json with managed identity authentication",
                        """
                                {
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "keyVaultScheme": "https",
                                  "keyVaultPort": 8080,
                                  "managedIdentity": {
                                    "targetResource": "https://example.com/",
                                    "identityServiceEndpoint": "http://localhost:8080"
                                  }
                                }
                                """,
                        new AzureKeyVaultConfig(null, new ManagedIdentityConfig("https://example.com/", URI.create("http://localhost:8080")), "my-key-vault",
                                "vault.azure.net", "https",
                                8080,
                                null)));
    }

    @MethodSource
    @ParameterizedTest
    void validJson(String json, AzureKeyVaultConfig expectedSerializedObject) throws IOException {
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        assertThat(config).isEqualTo(expectedSerializedObject);
    }

    public static Stream<Arguments> jsonFidelity() {
        return Stream.of(
                argumentSet("minimum json fidelity with entra identity authentication",
                        """
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
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net"
                                }
                                """),
                argumentSet("minimum json fidelity with managed identity authentication",
                        """
                                {
                                  "managedIdentity": {
                                    "targetResource": "https://example.com/"
                                  },
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net"
                                }
                                """),
                argumentSet("comprehensive json fidelity with entra identity authentication",
                        """
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
                                  "keyVaultScheme": "https",
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "keyVaultPort": 8080
                                }
                                """),
                argumentSet("comprehensive json fidelity with managed identity authentication",
                        """
                                {
                                  "managedIdentity": {
                                    "targetResource": "https://example.com/",
                                    "identityServiceEndpoint": "http://localhost:8080"
                                  },
                                  "keyVaultScheme": "https",
                                  "keyVaultName": "my-key-vault",
                                  "keyVaultHost": "vault.azure.net",
                                  "keyVaultPort": 8080
                                }
                                """));
    }

    @MethodSource
    @ParameterizedTest
    void jsonFidelity(String json) throws IOException {
        String normalized = mapper.writeValueAsString(mapper.readValue(json, JsonNode.class));
        AzureKeyVaultConfig config = mapper.readValue(json, AzureKeyVaultConfig.class);
        String actual = mapper.writeValueAsString(config);
        assertThat(actual).isEqualTo(normalized);
    }
}
