#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
	echo "Please run with sudo (as root): sudo $0"
	exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "[1/5] Updating apt index..."
apt-get update -y

echo "[2/5] Installing core build + test tooling..."
apt-get install -y \
	build-essential \
	make \
	cmake \
	ninja-build \
	pkg-config \
	git \
	curl \
	ca-certificates \
	python3 \
	python3-pip \
	clangd \
	rustc \
	cargo \
	libssl-dev \
	libcurl4-openssl-dev \
	zlib1g-dev \
	libzstd-dev \
	liblz4-dev \
	libbz2-dev

echo "[3/5] Installing Docker engine + compose plugin..."
apt-get install -y docker.io docker-compose-plugin

echo "[4/5] Enabling Docker service..."
systemctl enable --now docker

echo "[5/5] Adding invoking user to docker group (if applicable)..."
if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
	usermod -aG docker "${SUDO_USER}"
	echo "User '${SUDO_USER}' added to docker group."
	echo "IMPORTANT: log out and back in (or reboot) for group changes to apply."
else
	echo "No non-root SUDO_USER detected; skipping usermod."
fi

echo
echo "Done. Verify with:"
echo "  rustc --version && cargo --version"
echo "  cmake --version && ninja --version && g++ --version && make --version"
echo "  docker --version && docker compose version && docker ps"
echo "  python3 --version"
