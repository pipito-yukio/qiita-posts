#!/bin/bash

# ./gen_wpa_supplicant_conf.sh ssid encript_key

WPA_SUPP_CONF="${HOME}/bin/raspi/wpa_supplicant.conf"
SSID=
ENCRYPT_KEY=
if [ $# -eq 2 ]; then
   SSID=$1
   ENCRYPT_KEY=$2
else
   echo "SSID とパスワードが必要です。" >&2
   exit 1
fi

# パスワードの暗号化実行
WPA_PASS=$(echo "${ENCRYPT_KEY}" | wpa_passphrase "${SSID}" | grep -vE "^\s*#")

cat <<- CONF_EOF >${WPA_SUPP_CONF}
country=JP
ctrl_interface=DIR=/var/run/wpa_supplicant GROUP=netdev
ap_scan=1

update_config=1
${WPA_PASS}
CONF_EOF

