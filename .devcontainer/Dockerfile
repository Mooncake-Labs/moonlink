FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y \
    bison \
    flex \
    libclang-dev \
 && rm -rf /var/lib/apt/lists/*

USER vscode

RUN cargo install --locked cargo-pgrx \
 && cargo pgrx init

RUN echo '\\i /workspaces/pg_moonlink/.devcontainer/.psqlrc' > ~/.psqlrc
