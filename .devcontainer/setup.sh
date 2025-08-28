# VSCode devcontainer config.
git config devcontainers-theme.show-dirty 1

# first we ALTER the running postgres instance to use the TLS certs in /certs
psql "postgresql://postgres:postgres@postgres:5432/postgres" -c "ALTER SYSTEM SET ssl_cert_file = '/certs/server.crt';"
psql "postgresql://postgres:postgres@postgres:5432/postgres" -c "ALTER SYSTEM SET ssl_key_file = '/certs/server.key';"
psql "postgresql://postgres:postgres@postgres:5432/postgres" -c "ALTER SYSTEM SET ssl = on;"
psql "postgresql://postgres:postgres@postgres:5432/postgres" -c "SELECT pg_reload_conf();"

# pg access.
echo "alias psql='PGPASSWORD=postgres psql -h postgres -p 5432 -U postgres -d postgres'" >> ~/.bashrc

# precommit hook requirement.
rm -f .git/hooks/*
pre-commit install -t pre-push

cat >> ~/.bashrc <<EOF
function pgmooncake_server() {
    sudo rm -rf /var/lib/postgresql
    sudo install -d -o vscode -g vscode /var/lib/postgresql
    mkdir -p /var/lib/postgresql/data
    docker run --name mooncake --rm -e POSTGRES_PASSWORD=password -v /var/lib/postgresql/data:/var/lib/postgresql/data --user "$(id -u):$(id -g)" mooncakelabs/pg_mooncake
}

function pgmooncake_client() {
    docker exec -it mooncake psql -U postgres
}
EOF
