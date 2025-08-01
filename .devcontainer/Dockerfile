FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y python3-pip \
 && rm -rf /var/lib/apt/lists/*

USER vscode

RUN cd ~ \
 && git clone --branch stable https://github.com/rui314/mold.git \
 && cd mold \
 && sudo ./install-build-deps.sh \
 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -B build \
 && cmake --build build -j$(nproc) \
 && sudo cmake --build build --target install \
 && sudo ln -fs /usr/local/bin/mold /usr/bin/ld

RUN cargo install cargo-deny cargo-sort

RUN pip3 install --break-system-packages pre-commit
