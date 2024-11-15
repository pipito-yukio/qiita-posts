#!/bin/bash

# execute before export my_passwd=xxxxxx

# https://docs.docker.com/engine/install/ubuntu/
#   Install Docker Engine on Ubuntu

# Uninstall old versions
#  最小構成のUbuntu24.04 又はラズパイのUbuntu24.04ではインストールされていない
echo $my_passwd | {
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc
do
   sudo --stdin apt-get -y remove $pkg
done
}

echo $my_passwd | { sudo --stdin apt-get update
   sudo apt-get -y install ca-certificates curl
}

# Dockerの公式GPG鍵を追加する
echo $my_passwd | {
   sudo --stdin curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
   sudo chmod a+r /etc/apt/keyrings/docker.asc
}

# 安定版のリポジトリのセットアップ
echo $my_passwd | { echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo --stdin tee /etc/apt/sources.list.d/docker.list > /dev/null
}

# Docker Engine のインストール: + docker-buildx-plugin 
echo $my_passwd | { sudo --stdin apt-get update
   sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

# docker execute to raspi
echo $my_passwd | sudo --stdin gpasswd -a $USER docker

# Enable user apps system services
echo $my_passwd | { sudo --stdin cp ~/work/etc/default/postgresql-docker /etc/default
  sudo cp ~/work/etc/systemd/system/postgresql-docker.service /etc/systemd/system
  sudo systemctl enable postgresql-docker.service
}

echo "Done, logout this terminqal."

