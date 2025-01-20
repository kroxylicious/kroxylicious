/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static java.util.stream.Collectors.toSet;

/**
 * A {@link KeyProvider} backed by a private-key/certificate pair expressed in PEM format.
 * <br/>
 * Note that support for PKCS-8 private keys is derived from the JDK.  PKCS-1 private keys are only supported if Bouncy Castle
 * is available on the classpath.
 *
 * @param privateKeyFile      location of a file containing the private key.
 * @param certificateFile     location of a file containing the certificate and intermediates.
 * @param keyPasswordProvider provider for the privateKeyFile password or null if key does not require a password
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "The paths provide the location for key material which may exist anywhere on the file-system. Paths are provided by the user in the administrator role via Kroxylicious configuration. ")
public record KeyPair(@JsonProperty(required = true) String privateKeyFile,
                      @JsonProperty(required = true) String certificateFile,
                      @JsonProperty(value = "keyPassword") PasswordProvider keyPasswordProvider)
        implements KeyProvider {

    public static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    public static final String END_CERTIFICATE = "-----END CERTIFICATE-----";

    public KeyPair {
        Objects.requireNonNull(privateKeyFile);
        Objects.requireNonNull(certificateFile);
    }

    @Override
    public <T> T accept(KeyProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Test if this certificate has SANs matching a set of hostnames
     * @param hostnames hostnames
     * @return true if all hostnames match an SAN of this certificate
     */
    public boolean matchesHostnames(Set<String> hostnames) {
        Set<Pattern> patterns = hostnamePatterns();
        return hostnames.stream().allMatch(hostname -> patterns.stream().map(pattern -> pattern.matcher(hostname)).anyMatch(Matcher::matches));
    }

    private Set<Pattern> hostnamePatterns() {
        try {
            Path certPath = Path.of(certificateFile);
            String pemString = Files.readString(certPath);
            X509Certificate certificate = parsePemX509(pemString);
            Set<String> dnsSubjectAlternativeNames = getDnsSubjectAlternativeNames(certificate);
            // fallback to common name
            if (dnsSubjectAlternativeNames.isEmpty()) {
                String name = certificate.getSubjectX500Principal().getName();
                if (name.startsWith("CN=")) {
                    dnsSubjectAlternativeNames = Set.of(name.substring("CN=".length()));
                }
                else {
                    throw new RuntimeException("failed to extract common name from certificate");
                }
            }
            return dnsSubjectAlternativeNames.stream().map(KeyPair::toHostnamePattern).collect(toSet());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // need to handle wildcard (*.xyz) and literal (abc.xyz)
    private static Pattern toHostnamePattern(String san) {
        if (san.startsWith("*.")) {
            String remnant = san.substring(2);
            return Pattern.compile(".*\\." + Pattern.quote(remnant));
        }
        return Pattern.compile(Pattern.quote(san));
    }

    private static Set<String> getDnsSubjectAlternativeNames(X509Certificate certificate) throws CertificateParsingException {
        Collection<List<?>> subjectAlternativeNames = certificate.getSubjectAlternativeNames();
        if (subjectAlternativeNames == null) {
            return Set.of();
        }
        // type 2 is dNSName IA5String
        return subjectAlternativeNames.stream().filter(san -> ((int) san.get(0)) == 2).map(san -> (String) san.get(1)).collect(toSet());
    }

    private static X509Certificate parsePemX509(String pemString) {
        try {
            int start = pemString.indexOf(BEGIN_CERTIFICATE);
            int end = pemString.indexOf(END_CERTIFICATE);
            String certBase64 = pemString.substring(start + BEGIN_CERTIFICATE.length(), end).replace("\n", "").strip();
            byte[] decoded = Base64.getDecoder().decode(certBase64);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate cert = cf.generateCertificate(new ByteArrayInputStream(decoded));
            if (cert instanceof X509Certificate) {
                return (X509Certificate) cert;
            }
            else {
                throw new RuntimeException("cert is not X509Certificate");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
