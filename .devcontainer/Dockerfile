FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update && \
    apt install -y python3-pip && \
    pip3 install pre-commit --break-system-packages && \
    rm -rf /var/lib/apt/lists/*

# Switch to vscode user for cargo installations, otherwise later cargo commands required root permission.
USER vscode

RUN cargo install cargo-sort cargo-deny

WORKDIR /workspaces/moonlink
