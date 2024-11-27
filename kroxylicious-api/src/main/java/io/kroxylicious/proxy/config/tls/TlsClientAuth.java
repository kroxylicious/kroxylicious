package io.kroxylicious.proxy.config.tls;

public enum TlsClientAuth {

    REQUIRED("required", "REQUIRE"),
    REQUESTED("requested", "OPTIONAL"),
    NONE("none", "NONE");

    private final String clientAuth;
    private final String nettyClientAuth;

    TlsClientAuth(String clientAuth, String nettyClientAuth) {
        this.clientAuth = clientAuth;
        this.nettyClientAuth = nettyClientAuth;
    }

    public String getClientAuth() {
        return this.clientAuth;
    }

    public String getNettyClientAuth() {
        return this.nettyClientAuth;
    }

}
