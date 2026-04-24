default:
    just --list

# Format all files.
fmt:
    cargo fmt --all

# Check the formatting of all files.
fmt-check:
    cargo fmt --all -- --check

# Lint the package.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Run all tests.
test:
    cargo test --all

# Perform a release build.
build:
    cargo build --release

# Check the package for formatting, linting and testing errors.
check: fmt-check lint test

run *ARGS:
    cargo run -- {{ARGS}}

# Build the wheel in 'out_dir'
wheel out_dir="dist":
    uv build --wheel --out-dir {{out_dir}}

# Build the source distribution in 'out_dir'
sdist out_dir="dist":
    uv build --sdist --out-dir {{out_dir}}
