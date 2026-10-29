#!/usr/bin/env bash
# Day 3C, the box side: Docker Engine, UTC, the clone. Run once on a fresh
# Ubuntu 24.04 instance as ubuntu. Secrets are not here; they arrive by scp
# afterwards (.env without AWS keys, keys/rsa_key.p8), never by commit.
set -euo pipefail

sudo timedatectl set-timezone UTC

# Docker Engine and the compose plugin from Docker's own apt repository, as
# docs.docker.com/engine/install/ubuntu describes.
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker ubuntu

# The AWS CLI, only so `aws sts get-caller-identity` can show the instance role.
sudo snap install aws-cli --classic

# The repository, read only over HTTPS; it is public.
if [ ! -d "$HOME/weather/.git" ]; then
  git clone https://github.com/pwhite0313/weather-data-pipeline.git "$HOME/weather"
fi
mkdir -p "$HOME/weather/keys"

echo "bootstrap done: $(docker --version); $(docker compose version); tz $(timedatectl show -p Timezone --value)"
echo "log out and back in (or newgrp docker) before using docker without sudo"
