For simplicity of the setup we are going to commit the generated `wasm` policies.

## Requirements

- [OPA Cli](https://github.com/open-policy-agent/opa/releases)
- tar/gzip

## Build a rule

```
opa build policy.rego -o bundle.tar.gz -t wasm
tar -xvf bundle.tar.gz
```

but keep only the `.wasm`, you can use the `build.sh` script for convenience.
