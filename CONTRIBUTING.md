# Contributing

## Environment Setup

The easiest way to start contributing is by using our **Dev Container**.
You can choose between two approaches:

### 1. Local VS Code (Recommend)

**Prerequisites:**
- [Docker](https://docs.docker.com/get-docker/) (or Podman)
- [VS Code](https://code.visualstudio.com/)
- [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

**Steps:**

1. Clone the repository
    ```bash
    git clone https://github.com/Mooncake-Labs/moonlink.git
    cd moonlink
    ```
2. Open in VS Code
3. Inside VS Code, run the following from the `Command Palette` to get started
    ```
    > Dev Containers: Open Folder in Container
    ```

    This will create an isolated Workspace in vscode.

### 2. GitHub Codespaces

You just need to [create a new codespace](https://codespace.new/Mooncake-Labs/pg_mooncake).

---

## Testing
Moonlink is a standard Rust project, and tests can be run using `cargo test`. By default, this will run all tests that don't require optional features.

Within the devcontainer, local GCS and S3 storage instances have been setup. To run the full test suite, including tests behind optional features such as `storage-gcs` and `storage-s3`, you can specify the features explicitly:
```sh
# Run tests with GCS support.
cargo test --features storage-gcs

# Run tests with S3 support.
cargo test --features storage-s3
```

---

## Formatting
Formatting and linting is configured properly in devcontainer via [precommit hooks](https://github.com/Mooncake-Labs/moonlink/blob/main/.pre-commit-config.yaml), which automatically triggers before you push a commit.
