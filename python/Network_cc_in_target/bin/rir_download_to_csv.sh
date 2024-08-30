#!/bin/bash

# 実行日時点の最新のRIRデータをダウンロードしインポート用のCSVファイルを出力する 
# Usage: 引数なしで実行
# bin/rir_download_to_csv.sh
# [保存先]
#  $HOME/Documents/public/RIR
# [作業日ディレクトリ]
#  mkdir -p $HOME/Documents/public/RIR/YYYY-mm-dd
# [インポート用CSV出力]
#  $HOME/Documents/public/RIR/csv/ipv4-all-YYYY-mm-dd.csv

# Exit on error
set -e

# 保存先ディレクトリ
SAVE_DIR="$HOME/Documents/public/RIR"

# RIRレジストリの最新データのリンク
APNIC_URL=https://ftp.apnic.net/stats/apnic/delegated-apnic-latest
AFRINIC_URL=https://ftp.apnic.net/stats/afrinic/delegated-afrinic-latest
ARIN_URL=https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest
LACNIC_URL=https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-latest
RIPENCC_URL=https://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest.txt
DL_URLS=($APNIC_URL $AFRINIC_URL $ARIN_URL $LACNIC_URL $RIPENCC_URL)

# 最新データの保存ファイル名
DL_APNIC="delegated-apnic-latest.txt"
DL_AFRINIC="delegated-afrinic-latest.txt"
DL_ARIN="delegated-arin-extended-latest.txt"
DL_LACNIC="delegated-lacnic-latest.txt"
DL_RIPENCC="delegated-ripencc-extended-latest.txt"
DL_FILES=($DL_APNIC $DL_AFRINIC $DL_ARIN $DL_LACNIC $DL_RIPENCC)
# 前処理済みファイル名
PRE_APNIC="ipv4-allocated-apnic.txt"
PRE_AFRINIC="ipv4-allocated-afrinic.txt"
PRE_ARIN="ipv4-allocated-arin.txt"
PRE_LACNIC="ipv4-allocated-lacnic.txt"
PRE_RIPENCC="ipv4-allocated-ripencc.txt"
PRE_FILES=($PRE_APNIC $PRE_AFRINIC $PRE_ARIN $PRE_LACNIC $PRE_RIPENCC)
# CSVファイル名
CSV_APNIC="ipv4-1-apnic.csv"
CSV_AFRINIC="ipv4-2-afrinic.csv"
CSV_ARIN="ipv4-3-arin.csv"
CSV_LACNIC="ipv4-4-lacnic.csv"
CSV_RIPENCC="ipv4-5-ripencc.csv"
CSV_FILES=($CSV_APNIC $CSV_AFRINIC $CSV_ARIN $CSV_LACNIC $CSV_RIPENCC)
# 一時CSVファイ名
CSV_TEMP="ipv4-all.csv"

# 作業日のサブディレクトリを作成
today=$(date +'%F')
# インポートCSVファイル名
today_csv="ipv4-all-$today.csv"
new_dir=$SAVE_DIR/$today
mkdir -p $new_dir

# サブディレクトリでRIRファイルのダウンロードと各種ファイルを生成する
cd $new_dir
# ダウンロード
for rir_no in $(seq 0 4)
do
   echo "Getting ${DL_URLS[$rir_no]}..."
   wget -O ${DL_FILES[$rir_no]} ${DL_URLS[$rir_no]}
done

# 前処理
for rir_no in $(seq 0 4)
do
   grep -E "^[a-z]+\|[A-Z]{2}\|ipv4\|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\|[1-9][0-9]+\|[1|2][0-9]{7}\|allocated\|*.*$" \
 ${DL_FILES[$rir_no]} | tr '|' '\t' > ${PRE_FILES[$rir_no]}
done

# 前処理ファイルをそれぞれのCSVファイルに変換
awk '{ printf("\"%s\",%d,\"%s\",\"%s\",1\n", $4,$5,$2,$6) }' ${PRE_FILES[0]} > ${CSV_FILES[0]}
awk '{ printf("\"%s\",%d,\"%s\",\"%s\",2\n", $4,$5,$2,$6) }' ${PRE_FILES[1]} > ${CSV_FILES[1]}
awk '{ printf("\"%s\",%d,\"%s\",\"%s\",3\n", $4,$5,$2,$6) }' ${PRE_FILES[2]} > ${CSV_FILES[2]}
awk '{ printf("\"%s\",%d,\"%s\",\"%s\",4\n", $4,$5,$2,$6) }' ${PRE_FILES[3]} > ${CSV_FILES[3]}
awk '{ printf("\"%s\",%d,\"%s\",\"%s\",5\n", $4,$5,$2,$6) }' ${PRE_FILES[4]} > ${CSV_FILES[4]}

# ヘッダー出力
echo "\"ip_start\",\"ip_count\",\"country_code\",\"allocated_date\",\"registry_id\"" > $CSV_TEMP

# CSVファイルに各RIRデータを追記する
cat ${CSV_FILES[0]} ${CSV_FILES[1]} ${CSV_FILES[2]} ${CSV_FILES[3]} ${CSV_FILES[4]} >>$CSV_TEMP

# 保存ディレクトリに戻り
cd $SAVE_DIR
# 作業日にリネームして保存
mv $new_dir/$CSV_TEMP csv/$today_csv
echo "Saved: csv/$today_csv"

