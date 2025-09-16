/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import static java.nio.charset.StandardCharsets.UTF_8;

final class TestData {
    //
    // SASL PLAIN - Known good from https://datatracker.ietf.org/doc/html/rfc4616
    //
    static final byte[] SASL_PLAIN_CLIENT_INITIAL = "\0tim\0tanstaaftanstaaf".getBytes(UTF_8);
    static final byte[] SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID = "Ursel\0Kurt\0xipj3plmq".getBytes(UTF_8);

    //
    // SCRAM-SHA-256 - Known good from https://datatracker.ietf.org/doc/html/rfc7677
    //
    static final byte[] SASL_SCRAM_SHA_256_CLIENT_INITIAL = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(UTF_8);
    static final byte[] SASL_SCRAM_SHA256_CLIENT_INITIAL_WITH_AUTHZID = "n,a=Ursel,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(UTF_8);
    static final byte[] SASL_SCRAM_SHA_256_SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes(
            UTF_8);
    static final byte[] SASL_SCRAM_SHA_256_CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
            .getBytes(UTF_8);
    static final byte[] SASL_SCRAM_SHA_256_SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".getBytes(UTF_8);

    //
    // SCRAM-SHA-512 (test data not from authoritative source).
    //

    static final byte[] SASL_SCRAM_SHA_512_CLIENT_INITIAL = SASL_SCRAM_SHA_256_CLIENT_INITIAL;
    static final byte[] SASL_SCRAM_SHA_512_SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,s=Yin2FuHTt/M0kJWb0t9OI32n2VmOGi3m+JfjOvuDF88=,i=4096"
            .getBytes(UTF_8);
    static final byte[] SASL_SCRAM_SHA_512_CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,p=Hc5yec3NmCD7t+kFRw4/3yD6/F3SQHc7AVYschRja+Bc3sbdjlA0eH1OjJc0DD4ghn1tnXN5/Wr6qm9xmaHt4A=="
            .getBytes(UTF_8);
    static final byte[] SASL_SCRAM_SHA_512_SERVER_FINAL = "v=BQuhnKHqYDwQWS5jAw4sZed+C9KFUALsbrq81bB0mh+bcUUbbMPNNmBIupnS2AmyyDnG5CTBQtkjJ9kyY4kzmw==".getBytes(
            UTF_8);

    private TestData() {
        //
    }
}
