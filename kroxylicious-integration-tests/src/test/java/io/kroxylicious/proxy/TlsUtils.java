/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateEncodingException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class TlsUtils {

    public static Path convertToTempFileInPemFormat(Path brokerTruststore, String password) {
        try {
            var trustStore = KeyStore.getInstance(brokerTruststore.toFile(), password.toCharArray());
            var params = new PKIXParameters(trustStore);

            var trustAnchors = params.getTrustAnchors();
            var certificates = trustAnchors.stream().map(TrustAnchor::getTrustedCert).toList();
            assertThat(certificates).isNotNull()
                    .hasSizeGreaterThan(0);
            return TlsUtils.writeTrustToTempFileInPemFormat(certificates);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private static Path writeTrustToTempFileInPemFormat(List<X509Certificate> certificates) {
        try {
            var file = File.createTempFile("trust", ".pem");
            makeFileOwnerReadWriteOnly(file);
            file.deleteOnExit();
            var mimeLineEnding = new byte[]{ '\r', '\n' };

            try (var out = new FileOutputStream(file)) {
                certificates.forEach(c -> {
                    var encoder = Base64.getMimeEncoder();
                    try {
                        out.write("-----BEGIN CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                        out.write(encoder.encode(c.getEncoded()));
                        out.write(mimeLineEnding);
                        out.write("-----END CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    catch (CertificateEncodingException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(file.setWritable(false, true)).isTrue();
                return file.toPath();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write trust to temporary file", e);
        }
    }

    @NonNull
    public static File writePasswordToTempFile(String password) {
        try {
            File tmp = File.createTempFile("password", ".txt");
            tmp.deleteOnExit();
            makeFileOwnerReadWriteOnly(tmp);
            boolean ignore;
            Files.writeString(tmp.toPath(), password);
            // remove write from owner
            assertThat(tmp.setWritable(false, true)).isTrue();
            return tmp;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write password to file", e);
        }
    }

    private static void makeFileOwnerReadWriteOnly(File f) {
        // remove read/write from everyone
        assertThat(f.setReadable(false, false)).isTrue();
        assertThat(f.setWritable(false, false)).isTrue();
        // add read/write for owner
        assertThat(f.setReadable(true, true)).isTrue();
        assertThat(f.setWritable(true, true)).isTrue();
    }
}
