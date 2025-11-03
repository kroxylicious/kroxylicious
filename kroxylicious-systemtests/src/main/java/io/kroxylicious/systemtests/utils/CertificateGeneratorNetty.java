/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * The type Certificate generator netty.
 */
public class CertificateGeneratorNetty {
    private final String domain;
    private final String ipAddress;
    private String password;
    private Path keyStoreFilePath;

    /**
     * Instantiates a new Certificate generator netty.
     *
     * @param domain the domain
     */
    public CertificateGeneratorNetty(String domain) {
        this(domain, null);
    }

    /**
     * Instantiates a new Certificate generator netty.
     *
     * @param domain the domain
     * @param ipAddress the ip address
     */
    public CertificateGeneratorNetty(String domain, String ipAddress) {
        this.domain = domain;
        this.ipAddress = ipAddress;
    }

    private String getDomainName(String email, String organizationUnit, String organization, String city, String state, String country) {
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state + ", C=" + country + ", EMAILADDRESS=" + email;
    }

    /**
     * Generate self-signed certificate entry.
     *
     * @param email the email
     * @param organizationUnit the organization unit
     * @param organization the organization
     * @param city the city
     * @param state the state
     * @param country the country
     * @throws Exception the exception
     */
    public void generateSelfSignedCertificateEntry(String email, String organizationUnit, String organization, String city, String state, String country)
            throws Exception {
        Instant now = Instant.now();
        CertificateBuilder template = new CertificateBuilder()
                .notBefore(now.minus(1, DAYS))
                .notAfter(now.plus(1, DAYS));
        var temp = template.copy()
                .subject(getDomainName(email, organizationUnit, organization, city, state, country))
                .rsa2048()
                .setIsCertificateAuthority(true)
                .addSanDnsName(domain);

        if (ipAddress != null) {
            temp.addSanIpAddress(ipAddress);
        }

        X509Bundle leaf = temp.buildSelfSigned();
        KeyStore keyStore = leaf.toKeyStore(getPassword().toCharArray());
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
        Path certsDirectory = Files.createTempDirectory("kroxylicious", attr);
        keyStoreFilePath = Paths.get(certsDirectory.toAbsolutePath().toString(), "keystore.jks");
        try (FileOutputStream stream = new FileOutputStream(keyStoreFilePath.toFile())) {
            keyStore.store(stream, getPassword().toCharArray());
        }
        keyStoreFilePath.toFile().deleteOnExit();
        certsDirectory.toFile().deleteOnExit();
    }

    /**
     * Gets keystore location.
     *
     * @return  the keystore location
     */
    public String getKeyStoreLocation() {
        return keyStoreFilePath.toAbsolutePath().toString();
    }

    /**
     * Gets password.
     *
     * @return  the password
     */
    public String getPassword() {
        if (this.password == null) {
            this.password = UUID.randomUUID().toString().replace("-", "");
        }

        return this.password;
    }

    /**
     * Gets truststore location.
     *
     * @return  the truststore location
     */
    public String getTrustStoreLocation() {
        return getKeyStoreLocation();
    }
}
