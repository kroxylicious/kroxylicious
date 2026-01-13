#! /bin/bash
set -euxo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

(
    cd ${SCRIPT_DIR}
    mkdir -p tmp
    opa build policy.rego -o tmp/bundle.tar.gz -t wasm
    cd tmp
    tar -xvf bundle.tar.gz
    cd ..
    cp tmp/policy.wasm .
    rm -rf tmp
)

