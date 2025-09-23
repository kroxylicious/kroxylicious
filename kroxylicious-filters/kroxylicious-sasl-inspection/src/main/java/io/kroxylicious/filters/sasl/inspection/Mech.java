/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;

import edu.umd.cs.findbugs.annotations.NonNull;

enum Mech {

    /**
     * This a bogus SASL mechanism used when the proxy receives a handshake for a mechanism
     * that the proxy doesn't recognise. It sends this bogus mechanism (which we hope won't ever
     * be registered) to elicit from the server the mechanism(s) it supports.
     */
    PROBE_UPSTREAM(0) {
        @Override
        public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
            throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
        }

        @Override
        public String authorizationId(SaslAuthenticateRequestData request) {
            throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
        }

        @Override
        public boolean isFinished(int numAuthenticateSeen) {
            throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
        }
    },

    PLAIN(1) {
        @Override
        public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
            return numAuthenticateSeen == 1;
        }

        /* This function originally copied from Apach Kafka's PlainSaslServer */
        private static List<String> parsePlainClient(String string) {
            /*
             * Message format (from https://tools.ietf.org/html/rfc4616):
             *
             * message = [authzid] UTF8NUL authcid UTF8NUL passwd
             * authcid = 1*SAFE ; MUST accept up to 255 octets
             * authzid = 1*SAFE ; MUST accept up to 255 octets
             * passwd = 1*SAFE ; MUST accept up to 255 octets
             * UTF8NUL = %x00 ; UTF-8 encoded NUL character
             *
             * SAFE = UTF1 / UTF2 / UTF3 / UTF4
             * ;; any UTF-8 encoded Unicode character except NUL
             */
            List<String> tokens = new ArrayList<>();
            int startIndex = 0;
            for (int i = 0; i < 4; ++i) {
                int endIndex = string.indexOf("\u0000", startIndex);
                if (endIndex == -1) {
                    tokens.add(string.substring(startIndex));
                    break;
                }
                tokens.add(string.substring(startIndex, endIndex));
                startIndex = endIndex + 1;
            }

            if (tokens.size() != 3) {
                throw new SaslAuthenticationException("Invalid SASL/PLAIN response: expected 3 tokens, got " +
                        tokens.size());
            }

            return tokens;
        }

        @Override
        public String authorizationId(SaslAuthenticateRequestData request) {
            var tokens = parsePlainClient(new String(request.authBytes(), StandardCharsets.UTF_8));
            String authorizationIdFromClient = tokens.get(0);
            String username = tokens.get(1);
            return authorizationIdFromClient.isEmpty() ? username : authorizationIdFromClient;
        }

        @Override
        public boolean isFinished(int numAuthenticateSeen) {
            if (numAuthenticateSeen == 1) {
                return true;
            }
            throw new AuthenticationException("Illegal state: nextState called on " + this + " mech with numAuthenticateSeen=" + numAuthenticateSeen);
        }
    },

    SCRAM_SHA_256(2) {
        @Override
        public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
            return numAuthenticateSeen == 1;
        }

        @NonNull
        private List<String> parseScramClientFirst(String clientFirst) {
            List<String> tokens = new ArrayList<>(4);
            int startIndex = 0;
            for (int i = 0; i < 4; ++i) {
                int endIndex = clientFirst.indexOf(",", startIndex);
                if (endIndex == -1) {
                    tokens.add(clientFirst.substring(startIndex));
                    break;
                }
                tokens.add(clientFirst.substring(startIndex, endIndex));
                startIndex = endIndex + 1;
            }
            if (tokens.size() != 4) {
                throw new SaslAuthenticationException("Invalid SASL/" + this.mechanismName() + " response: expected 4 tokens, got " +
                        tokens.size());
            }
            return tokens;
        }

        @Override
        public String authorizationId(SaslAuthenticateRequestData request) {
            // n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
            // n,a=ursel,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL

            var clientFirst = new String(request.authBytes(), StandardCharsets.UTF_8);
            List<String> tokens = parseScramClientFirst(clientFirst);
            String username = tokens.get(2).startsWith("n=") ? tokens.get(2).substring(2) : null;
            if (username == null) {
                throw new SaslAuthenticationException("Invalid SCRAM client first message, username (n) absent");
            }
            String authzid = tokens.get(1).startsWith("a=") ? tokens.get(1).substring(2) : "";
            return ScramFormatter.username(authzid.isEmpty() ? username : authzid);
        }

        @Override
        public boolean isFinished(int numAuthenticateSeen) {
            if (numAuthenticateSeen == 1) {
                return false;
            }
            if (numAuthenticateSeen == 2) {
                return true;
            }
            throw new AuthenticationException("Illegal state: nextState called on " + this + " mech with numAuthenticateSeen=" + numAuthenticateSeen);
        }
    },

    SCRAM_SHA_512(2) {
        @Override
        public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
            // All SCRAM mechs are the same
            return SCRAM_SHA_256.requestContainsAuthorizationId(numAuthenticateSeen);
        }

        @Override
        public String authorizationId(SaslAuthenticateRequestData request) {
            // All SCRAM mechs are the same
            return SCRAM_SHA_256.authorizationId(request);
        }

        @Override
        public boolean isFinished(int numAuthenticateSeen) {
            // All SCRAM mechs are the same
            return SCRAM_SHA_256.isFinished(numAuthenticateSeen);
        }
    };
    public static final Set<String> SUPPORTED_MECHANISMS = Set.of(
            PLAIN.mechanismName(),
            SCRAM_SHA_256.mechanismName(),
            SCRAM_SHA_512.mechanismName());

    private final int numAuthenticateRequests;

    Mech(int numAuthenticateRequests) {
        this.numAuthenticateRequests = numAuthenticateRequests;
    }

    static Mech fromMechanismName(String mechanism) {
        return Mech.valueOf(mechanism.replace('-', '_'));
    }

    boolean isLastSaslAuthenticateResponse(int numAuthenticateSeen) {
        return numAuthenticateSeen == this.numAuthenticateRequests;
    }

    String mechanismName() {
        return this.name().replace('_', '-');
    }

    public abstract boolean isFinished(int numAuthenticateSeen);

    public abstract String authorizationId(SaslAuthenticateRequestData request);

    public abstract boolean requestContainsAuthorizationId(int numAuthenticateSeen);
}
