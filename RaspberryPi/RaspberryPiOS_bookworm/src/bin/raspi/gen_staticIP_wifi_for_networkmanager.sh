#!/bin/bash

# ./gen_staticIP_wifi_for_networkmanager.sh id_name ip_address
# [example]
# ./gen_staticIP_wifi_for_networkmanager.sh myssid_staticIp 192.168.0.41

if [ $# -ne 2 ]; then
   SCRIPT_NAME=${0##*/}
   echo "Usage: ./${SCRIPT_NAME} id_name ip_address"
   exit 1
fi    
ID_NAME=$1
IP_ADDR=$2

# Required wpa_supplicant.conf
WPA_SUPP_CONF="$HOME/bin/raspi/wpa_supplicant.conf"
SSID=
PSK=
if [ -f $WPA_SUPP_CONF ]; then
   # SSIDはダブルクォートを削除する
   SSID=$(cat wpa_supplicant.conf | grep "ssid" \
 | sed -n -e 's/.*ssid=//gp' | tr -d '"')
   PSK=$(cat ${WPA_SUPP_CONF} | grep "psk" \
 | sed -n 's/.*psk=//gp')
else
   echo "${WPA_SUPP_CONF} not found!"
   exit 1
fi    

CONNFILE=~/bin/raspi/connections/${ID_NAME}.connection
UUID=$(uuidgen)
TIMESTAMP=$(date +'%s')

cat <<- EOF >${CONNFILE}
[connection]
id=${ID_NAME}
uuid=${UUID}
type=wifi
interface-name=wlan0
timestamp=${TIMESTAMP}

[wifi]
mode=infrastructure
ssid=${SSID}

[wifi-security]
key-mgmt=wpa-psk
psk=${PSK}

[ipv4]
address1=${IP_ADDR}/24,192.168.0.1
dns=192.168.0.1;
method=manual

[ipv6]
addr-gen-mode=stable-privacy
method=auto

[proxy]
EOF

chmod 600 ${CONNFILE}
LANG=C;ls -l --time-style long-iso ${CONNFILE} | grep -v total
cat ${CONNFILE}

