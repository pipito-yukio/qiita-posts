#!/bin/bash

# execute before export my_passwd=xxxxxx
date +"%Y-%m-%d %H:%M:%S >Script START"

# 固定IPアドレスとホスト名を hosts ファイルに追加する
# OSインストール直後: ホスト名は [127.0.0.1  hostname] しか定義しない
# 外部PCにDockerコンテナ内で稼働するPostgreSQLをアクセス可能にするため
ip_addr=$(ifconfig eth0 | grep "inet " | awk '{ print $2 }')
host_in_hosts=$(cat /etc/hosts | grep 127.0.1.1 | awk '{ print $2 }')
host_in_hosts="${host_in_hosts}.local"
add_dot_host="${ip_addr}		${host_in_hosts}"
echo $my_passwd | { sudo --stdin chown pi.pi /etc/hosts
  echo $add_dot_host>>/etc/hosts
  sudo chown root.root /etc/hosts
}

# システムライブラリのインストール
# (1) sqlite3: ラズパイゼロ(本番機)から取得した気象データベース(sqlite3)をCSVに出力
# (2) docker-compose with doker engine: PostgreSQLコンテナ生成
echo $my_passwd | { sudo --stdin apt-get -y update
   sudo apt-get -y install sqlite3 docker-compose
}
exit1=$?
echo "Install system libraries >> status=$exit1"
if [ $exit1 -ne 0 ]; then
   echo "Fail install system libraries!" 1>&2
   exit $exit1
fi

# piユーザがsudoなしでdocker管理コマンドを実行可能にする: dockerグループに追加
echo $my_passwd | sudo --stdin gpasswd -a $USER docker

# PostgreSQLコンテナサービスの起動・シャットダウンサーピスを構成する
# (1) Create docker container service: postgres-docker.service
# (2) Cleanup docker container service with shutdown: postgres-docker.service
echo $my_passwd | { sudo --stdin cp ~/work/etc/default/postgres-docker /etc/default
  sudo cp ~/work/etc/systemd/system/postgres-docker.service /etc/systemd/system
  sudo cp ~/work/etc/systemd/system/cleanup-postgres-docker.service /etc/systemd/system
  sudo systemctl enable postgres-docker.service
  sudo systemctl enable cleanup-postgres-docker.service
}

date +"%Y-%m-%d %H:%M:%S >Script END"
echo "Done, Require logout this terminqal!"

