/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import static org.assertj.core.api.Assertions.assertThat;

public class TlsTestConstants {
    public static final String JKS = "JKS";
    public static final String PKCS_12 = "PKCS12";
    public static final String PEM = Tls.PEM;

    static final PasswordProvider STOREPASS = new InlinePassword("storepass");
    static final PasswordProvider KEYPASS = new InlinePassword("keypass");
    static final PasswordProvider BADPASS = new InlinePassword("badpass");
    static final PasswordProvider KEYSTORE_FILE_PASSWORD = new FilePassword(getResourceLocationOnFilesystem("storepass.password"));
    static final PasswordProvider KEYPASS_FILE_PASSWORD = new FilePassword(getResourceLocationOnFilesystem("keypass.password"));
    @SuppressWarnings("java:S5738") // java:S5738 warns of the use of the deprecated class.
    static final PasswordProvider KEYSTORE_FILE_PASSWORD_FILE_PATH = new FilePasswordFilePath(getResourceLocationOnFilesystem("storepass.password"));
    @SuppressWarnings("java:S5738") // java:S5738 warns of the use of the deprecated class.
    static final PasswordProvider KEYPASS_FILE_PASSWORD_FILE_PATH = new FilePasswordFilePath(getResourceLocationOnFilesystem("keypass.password"));

    public static final String NOT_EXIST = "/does/not/exist";

    public static String getResourceLocationOnFilesystem(String resource) {
        var url = TlsTestConstants.class.getResource(resource);
        assertThat(url).isNotNull();
        return url.getFile();
    }

}
