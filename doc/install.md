# Install

## Prebuilt binary

```sh
uv tool install rust-arrs
```

The PyPI package `rust-arrs` ships the `arrs` binary; `pipx install rust-arrs`
works the same way.

## From crates.io

```sh
cargo install arrs-cli
```

The crate is named `arrs-cli`, the binary and the library are named `arrs`.

## From a clone

```sh
git clone https://github.com/jonasdedden/arrs
cd arrs
cargo install --path .

# Or run without installing:
cargo run --release -- head -n 5 dataset.lance
```

The repository uses [`just`](https://github.com/casey/just) for common tasks:
`just check` runs formatting, clippy, and the test suite; `just wheel` and
`just sdist` build the Python distributions.

## Shell completions

`arrs completions <shell>` writes a completion script to stdout for `bash`,
`zsh`, `fish`, `powershell`, and `elvish`. Put it where your shell looks for
completions:

```sh
# bash
arrs completions bash | sudo tee /etc/bash_completion.d/arrs > /dev/null

# zsh — into a directory on $fpath, with `fpath+=(~/.zfunc)` before `compinit`
arrs completions zsh > ~/.zfunc/_arrs

# fish
arrs completions fish > ~/.config/fish/completions/arrs.fish

# PowerShell — write a file and dot-source it from your profile
arrs completions powershell > $HOME\arrs.completion.ps1
Add-Content $PROFILE '. $HOME\arrs.completion.ps1'
```
