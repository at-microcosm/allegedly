# Allegedly

Some [public ledger](https://github.com/did-method-plc/did-method-plc) tools and services

Allegedly can

- Tail PLC ops to stdout: `allegedly tail | jq`
- Export PLC ops to weekly gzipped bundles: `allegdly bundle --dest ./some-folder`
- Dump bundled ops to stdout FAST: `allegedly backfill --source-workers 6 | pv -l > /ops-unordered.jsonl`

(add `--help` to any command for more info about it)

also can:

- Copy ops to postgres for a mirror running the [reference typescript implementation](https://github.com/did-method-plc/did-method-plc)


## install

for now you'll need rust installed locally. after cloning, run

```bash
cargo install --path . --bin allegedly
```
