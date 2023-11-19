#!/bin/bash

readonly SCRIPT_NAME=${0##*/}

VALID_OPTION="drop-constraint"

print_error()
{
   cat << END 1>&2
$SCRIPT_NAME: $1
Try --help option
END
}

print_help()
{
   cat << END
Usage: $SCRIP_NAME OPTIONS
Execute OPTIONS

-o drop-constraint: Optional CSV import with DROP CONSTRAINT over 10000 record.  
--help	display this help and exit

Example:
  $SCRIPT_NAME
  $SCRIPT_NAME -o drop-constraint
END
}

params=$(getopt -o 'o:' -l help -n "$SCRIPT_NAME" -- "$@")

# Check command exit status
if [[ $? -ne 0 ]]; then
  echo 'Try --help option for more information' 1>&2
  exit 1
fi
eval set -- "$params"

# init option value
input_option=

# Positional parameter count: $#
while [[ $# -gt 0 ]]
do
  case "$1" in
    -o)
      input_option=$2
      shift 2
      ;;
    --'help')
      print_help
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      echo 'Internal error!' >&2
      exit 1
      ;;
  esac
done

echo "input_option: ${input_option}"
drop_constraint=

if [ -n "$input_option" ]; then
   # 短いオプションが入力されていたら、完全一致チェック
   if [ "$VALID_OPTION" == "$input_option" ]; then
       # 有効 
       drop_constraint=true
   else
      # 入力エラー
      echo "Error: short option -o is not match 'drop-constraint'" 1>&2
      exit 1
   fi
fi   

echo "drop_constraint: ${drop_constraint}"

if [[ ${drop_constraint} == true ]]; then
  docker exec -it postgres sh -c "${HOME}/data/sql/weather/import_csv_with_drop_constraint.sh ${HOME}"
else
  docker exec -it postgres sh -c "${HOME}/data/sql/weather/import_csv.sh ${HOME}"
fi

