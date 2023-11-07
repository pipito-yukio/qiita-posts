#!/bin/bash

readonly SCRIPT_NAME=${0##*/}

print_help()
{
   cat << END
Usage: $SCRIP_NAME OPTIONS
Execute GetCSVFromWeather.py OPTIONS

--device-name: Required 'ESP module device name'
--date-from: Required SQL Criteria Start date in t_weahter.
--date-to: Required SQL Criteria End date in t_weahter.
--help	display this help and exit

Example:
[short options]
  $SCRIPT_NAME -d esp8266_1 -f 2021-08-01 -t 2021-09-30
[long options]
  $SCRIPT_NAME --device-name esp8266_1 --date-from 2021-08-01 --date-to 2021-09-30
END
}

print_error()
{
   cat << END 1>&2
$SCRIPT_NAME: $1
Try --help option
END
}

params=$(getopt -n "$SCRIPT_NAME" \
       -o d:f:t:\
       -l device-name: -l date-from: -l date-to: -l help \
       -- "$@")

# Check command status: $?
if [[ $? -ne 0 ]]; then
  echo 'Try --help option for more information' 1>&2
  exit 1
fi

eval set -- "$params"

device_name=
date_from=
date_to=

# Parse options
# Positional parameter count: $#
while [[ $# -gt 0 ]]
do
  case "$1" in
    -d | --device-name)
      device_name=$2
      shift 2
      ;;
    -f | --date-from)
      date_from=$2
      shift 2
      ;;
    -t | --date-to)
      date_to=$2
      shift 2
      ;;
    --help)
      print_help
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Internal Error"
      exit 1
      ;;
  esac
done

echo "$SCRIPT_NAME --device-name $device_name --date-from $date_from --date-to $date_to"

# Check required option: --device-name
if [ -z $device_name ]; then
  print_error "Required --device-name xxxxx"
  exit 1
fi
if [ -z $date_from ]; then
  print_error "Required --date-from iso-8601 date"
  exit 1
fi
if [ -z $date_to ]; then
  print_error "Required --date-to iso-8601 date"
  exit 1
fi

option_device_name="--device-name $device_name"
option_date_from="--date-from $date_from"
option_date_to="--date-to $date_to"

echo "pigpio/GetCSVFromWeather.py  $option_device_name $option_date_from $option_date_to"

. ~/py_venv/py37_pigpio/bin/activate

python pigpio/GetCSVFromWeather.py $option_device_name $option_date_from $option_date_to

deactivate
