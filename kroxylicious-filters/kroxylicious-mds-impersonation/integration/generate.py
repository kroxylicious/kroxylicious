#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Generate short-lived, separate test trust domains; never use these keys in production."""

import os
import secrets
import shutil
import subprocess
from pathlib import Path


BASE = Path(__file__).resolve().parent
OUT = BASE / "generated"


def run(*args):
    result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr)


def ca(name):
    key, cert = OUT / f"{name}.key", OUT / f"{name}.crt"
    run("openssl", "req", "-x509", "-newkey", "rsa:3072", "-nodes", "-days", "2",
        "-subj", f"/CN={name}", "-addext", "basicConstraints=critical,CA:TRUE",
        "-keyout", key, "-out", cert)
    return key, cert


def identity(name, authority, server=False):
    key, cert = OUT / f"{name}.key", OUT / f"{name}.crt"
    csr, extensions = OUT / f"{name}.csr", OUT / f"{name}.ext"
    extensions.write_text("basicConstraints=critical,CA:FALSE\n"
                          "keyUsage=critical,digitalSignature,keyEncipherment\n"
                          f"extendedKeyUsage={'serverAuth' if server else 'clientAuth'}\n"
                          + ("subjectAltName=DNS:localhost,DNS:broker,IP:127.0.0.1\n" if server else ""))
    run("openssl", "req", "-new", "-newkey", "rsa:3072", "-nodes", "-subj", f"/CN={name}",
        "-keyout", key, "-out", csr)
    run("openssl", "x509", "-req", "-in", csr, "-CA", authority[1], "-CAkey", authority[0],
        "-set_serial", str(secrets.randbits(128)), "-days", "2", "-extfile", extensions, "-out", cert)
    csr.unlink()
    extensions.unlink()
    return key, cert


def main():
    if OUT.exists():
        raise SystemExit("generated/ already exists; stop the test environment before regenerating identities")
    os.umask(0o077)
    OUT.mkdir()
    broker = OUT / "broker"
    broker.mkdir()
    password = secrets.token_urlsafe(32)
    password_file = OUT / "store-password"
    password_file.write_text(password)
    server_ca = ca("server-ca")
    proxy_ca = ca("proxy-ca")
    client_ca = ca("client-ca")
    server_key, server_cert = identity("server", server_ca, server=True)
    identity("proxy", proxy_ca)
    identity("admin", proxy_ca)
    identity("alice", client_ca)
    identity("bob", client_ca)
    run("openssl", "pkcs12", "-export", "-name", "server", "-inkey", server_key,
        "-in", server_cert, "-certfile", server_ca[1], "-out", broker / "server.p12",
        "-passout", f"file:{password_file}")
    run("keytool", "-importcert", "-noprompt", "-alias", "proxy-ca", "-file", proxy_ca[1],
        "-keystore", broker / "proxy-ca.p12", "-storetype", "PKCS12", "-storepass:file", password_file)
    run("openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:3072", "-out", broker / "signing-key.pem")
    run("openssl", "pkey", "-in", broker / "signing-key.pem", "-pubout", "-out", broker / "signing-public.pem")
    template = (BASE / "server.properties.in").read_text()
    (broker / "server.properties").write_text(template.replace("{{STORE_PASSWORD}}", password))
    (BASE / ".env").write_text(f"LOCAL_UID={os.getuid()}\nLOCAL_GID={os.getgid()}\n")
    # CA signing keys are no longer needed for running the test.
    for key, _ in (server_ca, proxy_ca, client_ca):
        key.unlink()
    shutil.copyfile(server_ca[1], broker / "server-ca.crt")
    print("Generated test identities and broker configuration under generated/ (git-ignored).")


if __name__ == "__main__":
    main()
