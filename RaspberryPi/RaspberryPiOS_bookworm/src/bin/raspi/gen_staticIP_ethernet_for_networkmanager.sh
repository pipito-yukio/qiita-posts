#!/bin/bash

# ./gen_staticIP_ethernet_for_networkmanager.sh id_name ip_address
# [example]
# ./gen_staticIP_ethernet_for_networkmanager.sh Wired_staticIp 192.168.0.26

if [ $# -ne 2 ]; then
   SCRIPT_NAME=${0##*/}
   echo "Usage: ./${SCRIPT_NAME} id_name ip_address"
   exit 1
fi    
ID_NAME=$1
IP_ADDR=$2

CONNFILE=~/bin/raspi/connections/${ID_NAME}.connection
UUID=$(uuidgen)
TIMESTAMP=$(date +'%s')

cat <<- EOF >${CONNFILE}
[connection]
id=Wired_staticIp
uuid=${UUID}
type=ethernet
autoconnect-priority=-999
interface-name=eth0
timestamp=${TIMESTAMP}

[ethernet]

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

