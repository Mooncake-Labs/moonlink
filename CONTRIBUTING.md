# Contributing

## Dev Container
The easiest way to start contributing is via our Dev Container. To open the project in vscode you will need the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers). For codespaces you will need to [create a new codespace](https://codespace.new/Mooncake-Labs/pg_mooncake).

With the extension installed you can run the following from the `Command Palette` to get started
```
> Dev Containers: Clone Repository in Container Volume...
```

In the subsequent popup paste the url to the repo and hit enter.
```
https://github.com/Mooncake-Labs/moonlink
```

This will create an isolated Workspace in vscode.

## Testing
Moonlink is a standard Rust project, and tests can be run using `cargo test`. By default, this will run all tests that don't require optional features.

Within the devcontainer, local GCS and S3 storage instances have been setup. To run the full test suite, including tests behind optional features such as `storage-gcs` and `storage-s3`, you can specify the features explicitly:
```sh
# Run tests with GCS support.
cargo test --features storage-gcs

# Run tests with S3 support.
cargo test --features storage-s3
```

## Formatting
Formatting and linting is configured properly in devcontainer via [precommit hooks](https://github.com/Mooncake-Labs/moonlink/blob/main/.pre-commit-config.yaml), which automatically triggers before you push a commit.
