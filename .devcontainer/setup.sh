# VSCode devcontainer config.
git config devcontainers-theme.show-dirty 1

# pg access.
echo alias psql=\'psql -h localhost -U postgres\' >> ~/.bashrc

# precommit hook requirement.
pre-commit install -t pre-push
