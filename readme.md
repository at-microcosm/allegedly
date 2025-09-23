# Allegedly

Some [public ledger](https://github.com/did-method-plc/did-method-plc) tools and services for servers

Allegedly can

- Tail PLC ops to stdout: `allegedly tail | jq`
- Export PLC ops to weekly gzipped bundles: `allegdly bundle --dest ./some-folder`
- Dump bundled ops to stdout FAST: `allegedly backfill --source-workers 6 | pv -l > /ops-unordered.jsonl`
- Wrap the reference PLC server and run it as a mirror:

    ```bash
    export ALLEGEDLY_WRAP_PG="postgresql://user:pass@pg-host:5432/plc-db"
    allegedly --upstream "https://plc.directory" mirror \
      --bind "0.0.0.0:8000" \
      --wrap "http://127.0.0.1:3000"
   ```

(add `--help` to any command for more info about it)


## install

```bash
cargo install allegedly
```

the version on crates might be behind while new features are under development.
to install the latest from source:

- make sure you have rust/rustup set up
- clone the repo
- install

    ```bash
    cargo install --path . --bin allegedly
    ```


## license

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.

`SPDX-License-Identifier: MIT OR Apache-2.0`
