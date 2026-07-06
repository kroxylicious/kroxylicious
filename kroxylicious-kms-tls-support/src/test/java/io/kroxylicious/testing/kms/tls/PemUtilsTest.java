/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/*
 * Copyright 2022 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/*
 * Adapted from Netty's PemReaderTest for testing PemUtils.
 * Uses inline PEM test data from Netty's test suite.
 */

package io.kroxylicious.testing.kms.tls;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("UnnecessaryParentheses") // Parentheses required for text blocks with .getBytes()
class PemUtilsTest {

    @Test
    void mustBeAbleToReadSingleCertificate() throws Exception {
        // Given
        byte[] cert = """
                -----BEGIN CERTIFICATE-----
                MIICqjCCAZKgAwIBAgIIEaz8uuDHTcIwDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxo
                b3N0MCAXDTIxMDYxNjE3MjYyOFoYDzk5OTkxMjMxMjM1OTU5WjAUMRIwEAYDVQQDDAlsb2NhbGhv
                c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCVjENomtpMqHkg1yJ/uYZgSWmf/0Gb
                U4yMDf30muPvMYb3gO6peEnoXa2b0WDOjLbLrcltp1YdjTlLhRRTYgDo9TAvHoUdoMGlTnfQtQne
                2o+/92bnlZTroRIjUT0lqSxQ6UNXcOi9tNqVD4tML3vk20fudwBur8Plx+3hOhM/v64GbV46k06+
                AblrFwBt9u6V0uIVtvgraOd+NgL4yNf594uND30mbB7Q7xe/Y6DiPhI6cVI/CbLlXVwKLvC5OziS
                JKZ7svP0K3DBRxk+dOD9pg4SdaAEQVtR734ZlDh1XJ+mZssuDDda3NGZAjpCU4rkeV/J3Tr5KKMD
                g3NEOmifAgMBAAEwDQYJKoZIhvcNAQELBQADggEBABejZGeRNCyPdIqac6cyAf99JPp5OySEMWHU
                QXVCHEbQ8Eh6hsmrXSEaZS2zy/5dixPb5Rin0xaX5fqZdfLIUc0Mw/zXV/RiOIjrtUKez15Ko/4K
                ONyxELjUq+SaJx2C1UcKEMfQBeq7O6XO60CLQ2AtVozmvJU9pt4KYQv0Kr+br3iNFpRuR43IYHyx
                HP7QsD3L3LEqIqW/QtYEnAAngZofUiq0XELh4GB0L8DbcSJIxfZmYagFl7c2go9OZPD14mlaTnMV
                Pjd+OkwMif5T7v+r+KVSmDSMQwa+NfW+V6Xngg5/bN3kWHdw9qFQGANojl9wsRVN/B3pu3Cc2XFD
                MmQ=
                -----END CERTIFICATE-----
                """.getBytes(StandardCharsets.US_ASCII);

        // When
        X509Certificate[] certificates = PemUtils.parseCertificateChain(cert);

        // Then
        assertThat(certificates).hasSize(1);
        X509Certificate x509 = certificates[0];
        assertThat(x509.getSubjectX500Principal().getName()).contains("CN=localhost");
        assertThat(x509.getIssuerX500Principal().getName()).contains("CN=localhost"); // self-signed
        assertThat(x509.getSigAlgName()).isEqualTo("SHA256withRSA");
        assertThat(x509.getVersion()).isEqualTo(3); // X.509 v3
    }

    @Test
    void mustBeAbleToReadMultipleCertificates() throws Exception {
        // Given
        byte[] certs = """
                -----BEGIN CERTIFICATE-----
                MIICqjCCAZKgAwIBAgIIEaz8uuDHTcIwDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxo
                b3N0MCAXDTIxMDYxNjE3MjYyOFoYDzk5OTkxMjMxMjM1OTU5WjAUMRIwEAYDVQQDDAlsb2NhbGhv
                c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCVjENomtpMqHkg1yJ/uYZgSWmf/0Gb
                U4yMDf30muPvMYb3gO6peEnoXa2b0WDOjLbLrcltp1YdjTlLhRRTYgDo9TAvHoUdoMGlTnfQtQne
                2o+/92bnlZTroRIjUT0lqSxQ6UNXcOi9tNqVD4tML3vk20fudwBur8Plx+3hOhM/v64GbV46k06+
                AblrFwBt9u6V0uIVtvgraOd+NgL4yNf594uND30mbB7Q7xe/Y6DiPhI6cVI/CbLlXVwKLvC5OziS
                JKZ7svP0K3DBRxk+dOD9pg4SdaAEQVtR734ZlDh1XJ+mZssuDDda3NGZAjpCU4rkeV/J3Tr5KKMD
                g3NEOmifAgMBAAEwDQYJKoZIhvcNAQELBQADggEBABejZGeRNCyPdIqac6cyAf99JPp5OySEMWHU
                QXVCHEbQ8Eh6hsmrXSEaZS2zy/5dixPb5Rin0xaX5fqZdfLIUc0Mw/zXV/RiOIjrtUKez15Ko/4K
                ONyxELjUq+SaJx2C1UcKEMfQBeq7O6XO60CLQ2AtVozmvJU9pt4KYQv0Kr+br3iNFpRuR43IYHyx
                HP7QsD3L3LEqIqW/QtYEnAAngZofUiq0XELh4GB0L8DbcSJIxfZmYagFl7c2go9OZPD14mlaTnMV
                Pjd+OkwMif5T7v+r+KVSmDSMQwa+NfW+V6Xngg5/bN3kWHdw9qFQGANojl9wsRVN/B3pu3Cc2XFD
                MmQ=
                -----END CERTIFICATE-----
                -----BEGIN CERTIFICATE-----
                MIICqjCCAZKgAwIBAgIIIsUS6UkDau4wDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxo
                b3N0MCAXDTIxMDYxNjE3MjYyOFoYDzk5OTkxMjMxMjM1OTU5WjAUMRIwEAYDVQQDDAlsb2NhbGhv
                c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCTZmahFZXB0Dv3N8t6gfXTeqhTxRng
                mIBBPmrbZBODrZm06vrR5KNhxB2FhWIq1Yu8xXXv8sO+PaO2Sw/h6TeslRJ4EkrNd9zmYhT2cJvP
                d1CtkX5EHyMZRUKj7Eg4eUO1k/+JnhMmaY+nUAG7fCtvs8pS9SEXbEqYW7S4AQ1oopbCAMqQekly
                KCdnjGlVhXwL2Lj2rr/uw1Fc2+WvY/leQGo0rbIqoc7OSAktsP+MXI6iQ1RWJOec15V6iFRzcdE3
                Q4ODSMZ/R8wm9DH+4hkeQNPMbcc1wlvVZpDZ/FZegr1XimcYcJr2AoAQf3Xe1yFKAtBMXCjCIGm8
                veCQ+xeHAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGyV+dib2MdpenbntKk7PdZEx+/vNl9cEpwL
                BfWmQN/j2RmHUxrUM+PVkLTgyCq8okdCKqCvraKwBkF6vlzp4u5CL4L323z+/uxAi6pzbcWnG1EH
                JpSkf1OhTUFu6UhLfpg3XqeiIujYdVZTpHr7KHVLRYUSQPprt4HjLZeCIg4P2pZ0yQ3SEBhVed89
                GMj/+O4jjvuZv5NQc57NpMIrE9fNINczLG1CPTgnhvqMP42W6ahBuexQUe4gP+jmB/BZmBYKoauU
                mPBKruq3mNuoXtbHufv5I7CFVXNgJ0/aT+lvEkQ4IlCIcJyvTgyUTOQVbqDp+SswymAIRowaRdxa
                7Ss=
                -----END CERTIFICATE-----
                """.getBytes(StandardCharsets.US_ASCII);

        // When
        X509Certificate[] certificates = PemUtils.parseCertificateChain(certs);

        // Then
        assertThat(certificates).hasSize(2);
    }

    @Test
    void mustBeAbleToReadPKCS8RsaPrivateKey() throws Exception {
        // Given: PKCS#8 format RSA key (BEGIN PRIVATE KEY) - from Netty test data
        byte[] pkcs8Key = ("""
                -----BEGIN PRIVATE KEY-----
                MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDb+HBO3C0URBKvDUgJHbhIlBye
                8X/cbNH3lDq3XOOFBz7L4XZKLDIXS+FeQqSAUMo2otmU+Vkj0KorshMjbUXfE1KkTijTMJlaga2M
                2xVVt21fRIkJNWbIL0dWFLWyRq7OXdygyFkIiW9b2/LYaePBgET22kbtHSCAEj+BlSf265+1rNxy
                AXBGGGccCKzEbcqASBKHOgVp6pLqlQAfuSy6g/OzGzces3zXRrGu1N3pBIzAIwCW429n52ZlYfYR
                0nr+REKDnRrPIIDsWASmEHhBezTD+v0qCJRyLz2usFgWY+7agUJE2yHHI2mTu2RAFngBilJXlMCt
                VwT0xGuQxkbHAgMBAAECggEBAJJdKaVfXWNptCDkLnVaYB9y5eRgfppVkhQxfiw5023Vl1QjrgjG
                hYH4zHli0IBMwXA/RZWZoFVzZ3dxoshk0iQPgGKxWvrDEJcnSCo8MGL7jPvh52jILp6uzsGZQBji
                bTgFPmOBS7ShdgZiQKD9PD2psrmqHZ1yTwjIm5cGfzQM8Y6tjm0xLBn676ecJNdS1TL10y9vmSUM
                Ofdkmeg9Z9TEK95lP2fF/NIcxCo0LF9JcHUvTuYBDnBH0XMZi0w0ZcRReMSdAZ2lLiXgBeCO53el
                2NIrtkRx+qOvLua9UfwO2h/0rs66ZeV0YuFCjv067nytyZf2zhU/QbCHRypzfrkCgYEA/facuAJs
                6MQKsNvhozoBeDRMkrZPMh8Sb0w50EqzIGz3pdms6UvCiggoMbhxKOwuYWZ689fBPGwm7x0RdwDO
                jyUuEbFnQFe+CpdHy6VK7vIQed1SwAcdTMDwCYbkJNglqHEB7qUYYTFLr8okGyWVdthUoh4IAubU
                TR3TFbGraDUCgYEA3bwJ/UNA5pHtb/nh4/dNL7/bRMwXyPZPpC5z+gjjgUMgsSRBz8+iPNTB4iSQ
                1j9zm+pnXGi35zWZcI4jvIcFusb08eS7xcZDb+7X2r2wenLNmyuTOa1812y233FicU+ah91fa9aD
                yUfTjj3GFawbgNNhMyWa3aEMV+c73t6sKosCgYEA35oQZhsMlOx2lT0jrzlVLeauPMZzeCfPbVrp
                1DDRAg2vBcFf8pCXmjyQVyaTy3oXY/585tDh/DclGIa5Z9O4CmSr6TwPMqGOW3jS58SC81sBkqqB
                Pz2EWJ3POjQgDyiYD3RgRSPrETf78azCmXw/2sGh0pMqbpOZ/MPzpDgoOLkCgYEAsdv4g09kCs75
                Dz34hRzErE2P+8JePdPdlEuyudhRbUlEOvNjWucpMvRSRSyhhUnGWUWP/V7+TRcAanmJjtsbrHOU
                3Udlm0HqrCmAubQ4kC/wXsx4Pua7Yi2RDvBrT4rT4LGgreaXNWhI+Srx7kZslUx5Bkbez3I0bXpM
                2vvwS/sCgYAducNt1KC4W7jzMWUivvuy5hQQmX/G0JHtu1pfv9cmA8agnc1I/r7xoirftuSG25Pm
                r+eP5SKbKb8ZQlp10JeBkNnk8eAG8OkQyBaECYDBadEr1/LK2LmIEjYKzKAjYQ4cX2KMtY271jjX
                WrzzXNqBdThFfMHiJE8k9xYmaLDKhQ==
                -----END PRIVATE KEY-----
                """).getBytes(StandardCharsets.US_ASCII);

        // When
        PrivateKey privateKey = PemUtils.parsePrivateKey(pkcs8Key, null);

        // Then
        assertThat(privateKey).isNotNull();
        assertThat(privateKey.getAlgorithm()).isEqualTo("RSA");
        assertThat(privateKey.getFormat()).isEqualTo("PKCS#8");
    }

    @Test
    void mustBeAbleToReadPKCS1RsaPrivateKey() throws Exception {
        // Given: PKCS#1 format RSA key (BEGIN RSA PRIVATE KEY) - traditional format
        byte[] pkcs1Key = ("""
                -----BEGIN RSA PRIVATE KEY-----
                MIICXgIBAAKBgQC7Ic1+osUw/w29o607wPWtCE4lPinuYG+l7HfxDiLabg/laxVU
                Tcev7HwLltUBx8r45lzB6LhGKy5xdeCEvZ3nKm5ZnCM408lX/R1jeFy2h+lzhIOU
                vrRfsRe/krbm0jwrY/mPPWKWqU+PVAd0fvhjG3GRrgYlu2/pzZjA+RB18wIDAQAB
                AoGBAKisym7oRvhoHjmezGp8/rWuM8osI12j/V9BK8fTpyTeamOvxzULOwBfGFzV
                40BMl68M7fU3UMqm56EL0Im15RpLq4aludygMvuISsAQUR7okdDFL+gWGmovrrRl
                bcmicJpbxJKIdVySEUJYJimXXB0/5cW41+3/8OpBsUArZRFxAkEA3lAcH+Usi9np
                5/daZGOW00CQW+R700NjE1F8IHTA/91wCJLEuyyqvQV9ytPYKBIYA+DoK/FCHj3D
                kcbZYYbcqwJBANd896kyeKojBvrxI0LvkBqVVj8j70Trd3ci6V8V7VzDYY73s6K5
                61GbFXFvD3kLo0kYDOoV7yewrELiyOKsO9kCQQCwUKbNoQvAava5M5MsNVPkfbtA
                NikCt9o28xRYBWEgTHZTRlvi+xz6xwUqPPOdbCRBxzk7yJ8grumRjzzOvY/7AkEA
                hVwKrcTVln3NARqhNvip1znawYLMvnt3WNzbTwRz/LfSNbeojanAL6Xp1GTmT4Rb
                To4619gxRP/66/4MUvRCqQJAX7YC1BsTLoQK0Ez6bxFYjNgaiVOOnDxLC9l95Vyy
                LMqrHn3z/S078tNouyAtWsX5NcH2Bqs3//OZQfoevpJglQ==
                -----END RSA PRIVATE KEY-----
                """).getBytes(StandardCharsets.US_ASCII);

        // When
        PrivateKey privateKey = PemUtils.parsePrivateKey(pkcs1Key, null);

        // Then
        assertThat(privateKey).isNotNull();
        assertThat(privateKey.getAlgorithm()).isEqualTo("RSA");
        // PKCS#1 is converted to PKCS#8 internally
        assertThat(privateKey.getFormat()).isEqualTo("PKCS#8");
    }

    @Test
    void mustRejectEncryptedPrivateKeyWithPassword() {
        // Given: encrypted PKCS#8 key with password - from Netty test data
        byte[] encryptedKey = ("""
                -----BEGIN ENCRYPTED PRIVATE KEY-----
                MIIE9jAoBgoqhkiG9w0BDAEDMBoEFDBlaUwB8TQ9ImbApCmAyVRTTX+kAgIIAASC
                BMhC8QFNyn0VbVp7I+R9Yvmr+Ksl0xZshGg3zaUN8/HRblNSS3gPiP673rmnhcU3
                PfSNFR9hOrTqdtd5i6Qq4HznECs81KBlqRNB9ihgy++ByFkf6GTzdfBA6zJInhNx
                qSWjUwpFtV4or1w/N23bTcpdGmjfdCSFBMQdbkIDgT7GaWxd3mCLxSbfVzF64tev
                x+V22nA/TR0VWnG+aj7aVbReK6VpepiCX7ZmQ5KehXAeB0SDrgT89kcz2VIfDxvE
                hkCymNTcJY/ETdPfTSiR+DSZvVJMgVmfk7j1toZZSnoMwl4IhlXmIPmDOUE465l3
                sNWLygkNKymTmMI5FTT1hChAIdsmeVTfDmVzNPK4HQi5gfEnTCy0uxj9U3HCZWr1
                Zlzmw7/430TRqNYSEJ/XkhFaV5V+6LfeZOyuwf2VJAs+CwNo+UYzEQqkW11JMqhA
                i9fz8bCNoy4/dyWbE/wEK8UPGif1rzCpoodBYeWTt0QtHcIokE3ylXWyTTarz7jV
                u9Rnbq4HAXYYEwPjLmWFQ6NeD/rx/t44oEAyekxS+ZPIHNTVXRLBH5Tl/LDkpK15
                -----END ENCRYPTED PRIVATE KEY-----
                """).getBytes(StandardCharsets.US_ASCII);
        char[] password = "password".toCharArray();

        // When/Then
        assertThatThrownBy(() -> PemUtils.parsePrivateKey(encryptedKey, password))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Encrypted private keys are not supported")
                .hasMessageContaining("JKS/PKCS12")
                .hasMessageContaining("openssl pkcs8 -topk8 -nocrypt");
    }

    @Test
    void mustRejectEncryptedPrivateKeyWithoutPassword() {
        // Given: encrypted key without password - should fail during parsing
        byte[] encryptedKey = ("""
                -----BEGIN ENCRYPTED PRIVATE KEY-----
                MIIE9jAoBgoqhkiG9w0BDAEDMBoEFDBlaUwB8TQ9ImbApCmAyVRTTX+kAgIIAASC
                BMhC8QFNyn0VbVp7I+R9Yvmr+Ksl0xZshGg3zaUN8/HRblNSS3gPiP673rmnhcU3
                -----END ENCRYPTED PRIVATE KEY-----
                """).getBytes(StandardCharsets.US_ASCII);

        // When/Then - fails because the encrypted key can't be parsed as unencrypted
        assertThatThrownBy(() -> PemUtils.parsePrivateKey(encryptedKey, null))
                .isInstanceOf(IOException.class);
    }

    @Test
    void mustRejectMalformedCertificate() {
        // Given
        byte[] malformed = "not a valid PEM file".getBytes(StandardCharsets.UTF_8);

        // When/Then
        assertThatThrownBy(() -> PemUtils.parseCertificateChain(malformed))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("found no certificates");
    }

    @Test
    void mustRejectMalformedPrivateKeyNullPassword() {
        // Given
        byte[] malformed = "not a valid PEM file".getBytes(StandardCharsets.UTF_8);

        // When/Then
        assertThatThrownBy(() -> PemUtils.parsePrivateKey(malformed, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("could not find a PKCS #8 private key");
    }

    @Test
    void mustRejectPrivateKeyWithMissingFooter() {
        // Given
        byte[] missingFooter = """
                -----BEGIN PRIVATE KEY-----
                MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDb+HBO3C0URBKv
                """.getBytes(StandardCharsets.US_ASCII);

        // When/Then
        assertThatThrownBy(() -> PemUtils.parsePrivateKey(missingFooter, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("could not find private key footer");
    }

    @Test
    void mustRejectPKCS1EcPrivateKey() {
        // Given
        byte[] pkcs1EcKey = ("""
                -----BEGIN EC PRIVATE KEY-----
                MHcCAQEEIHerzPcVTKQmBL/eBzVEo6AIVg2rXTy6YhjMBxkntDzFoAoGCCqGSM49
                AwEHoUQDQgAEbU4kz0QpW7MQvkqbijjDt/fu9T/FrjZuMd+8F9wSxwXrTfJhcHAA
                7ibbeqf4y9m9QTPuRKzOQXS8A0qlOYEhzA==
                -----END EC PRIVATE KEY-----
                """).getBytes(StandardCharsets.US_ASCII);

        // When/Then
        assertThatThrownBy(() -> PemUtils.parsePrivateKey(pkcs1EcKey, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to parse private key")
                .hasMessageContaining("PKCS#1 (RSA only)");
    }
}
