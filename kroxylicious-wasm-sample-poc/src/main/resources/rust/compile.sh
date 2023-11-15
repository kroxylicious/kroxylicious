#! /bin/bash
set -euxo pipefail
# rustup target add wasm32-unknown-unknown

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

rm -f ${SCRIPT_DIR}/../wasm/*

rustc fromto.rs --target=wasm32-unknown-unknown --crate-type=cdylib -C opt-level=0 -C debuginfo=0 -o ${SCRIPT_DIR}/../wasm/fromto.wasm
rustc foobar.rs --target=wasm32-unknown-unknown --crate-type=cdylib -C opt-level=0 -C debuginfo=0 -o ${SCRIPT_DIR}/../wasm/foobar.wasm
rustc barbaz.rs --target=wasm32-unknown-unknown --crate-type=cdylib -C opt-level=0 -C debuginfo=0 -o ${SCRIPT_DIR}/../wasm/barbaz.wasm
