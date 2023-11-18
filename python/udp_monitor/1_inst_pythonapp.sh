#!/bin/bash

# execute before export my_passwd=xxxxxx
date +"%Y-%m-%d %H:%M:%S >Script START"

# Install system libraries
# (1) python3-venv: make python virtual environment.
echo $my_passwd | { sudo --stdin apt-get -y update
   sudo apt-get -y install python3-venv
}
exit1=$?
echo "Install system libraries >> status=$exit1"
if [ $exit1 -ne 0 ]; then
   echo "Fail install system libraries!" 1>&2
   exit $exit1
fi

# Create Python virtual environment.
if [ ! -d "$HOME/py_venv" ]; then
   mkdir py_venv
fi

cd py_venv
python3 -m venv raspi4_apps
. raspi4_apps/bin/activate
# requirements.txt in xxxx libraries
# pip install -r ~/work/requirements.txt
exit1=$?
echo "Make python virtual environment raspi4_apps >> status=$exit1"
deactivate
if [ $exit1 -ne 0 ]; then
   echo "Fail make python virtual environment!" 1>&2
   exit $exit1
fi

cd ~/

# Enable my python application service
# UDP packet monitor servcie: udp-weather-mon.service
echo $my_passwd | { sudo --stdin cp ~/work/etc/default/udp-weather-mon /etc/default
  sudo cp ~/work/etc/systemd/system/udp-weather-mon.service /etc/systemd/system
  sudo systemctl enable udp-weather-mon.service
}
exit1=$?

date +"%Y-%m-%d %H:%M:%S >Script END"
if [ $exit1 -ne 0 ]; then
   echo "Fail import all csv!" 1>&2
   exit $exit1
else
   echo "Done"
   echo "rebooting."
   echo $my_passwd |sudo --stdin reboot
fi

