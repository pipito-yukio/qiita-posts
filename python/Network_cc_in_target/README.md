[Qiita] 「不正アクセスしてきたホストの国コードを知ってセキュリティ対策に活用する」で解説したソースコードと生成ファイル

ソースの構成
```
Network_cc_in_target/
├── README.md
├── bin
│   └── rir_download_to_csv.sh
├── output
│   └── match_networks
│       ├── ip_network_cc_with_hosts_2024-08-28.txt
│       └── unknown_ip_hosts_2024-08-28.txt
├── requirements.txt
├── scripts
│   ├── find_target_ip_like_param.sh
│   └── import_from_allocated_ipv4_csv.sh
├── sql
│   └── 14_add_ipv4_table.sql
└── src
    ├── IpNetworkCC_in_hosts.py
    ├── IpNetworkCC_in_hosts_with_csv.py
    ├── TestDetectCountryCode.py
    ├── conf
    │   ├── db_conn.json
    │   └── export_sql_with_ip_country_code.json
    ├── csv
    │   ├── ssh_auth_error_2024-06-10.csv
    │   └── ssh_auth_error_cc_match.csv
    ├── db
    │   └── pgdatabase.py
    └── mypy.ini
```

下記JSONファイル中で定義しているディレクトリ("output-dir"と"match-networks-dir")は作成済みであることを前提としています。

[src/conf/export_sql_with_ip_country_code.json]
```json
{
  "output-dir": "~/Documents/qiita/sql/batch",
  "query": {
    "match-networks-dir": "~/Documents/qiita/match_networks"
  }
}
```

5つのRIRレジストリから最新データをダウンロードしてインポート用CSVファイルを作成するスクリプト

※1 **wget** がインストール済みであること。  
※2 保存先ディレクトリ "SAVE_DIR" はお使いの環境に合わせて修正して下さい。

[bin/rir_download_to_csv.sh]
```shell
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
```

[実行結果]

```bash
~/Documents/public$ bin/rir_download_to_csv.sh 
Getting https://ftp.apnic.net/stats/apnic/delegated-apnic-latest...
--2024-08-30 16:40:05--  https://ftp.apnic.net/stats/apnic/delegated-apnic-latest
ftp.apnic.net (ftp.apnic.net) をDNSに問いあわせています... 203.119.102.40
ftp.apnic.net (ftp.apnic.net)|203.119.102.40|:443 に接続しています... 接続しました。
HTTP による接続要求を送信しました、応答を待っています... 200 OK
長さ: 3861030 (3.7M) [text/plain]
‘delegated-apnic-latest.txt’ に保存中

delegated-apnic-latest.txt       100%[==========================================================>]   3.68M   107KB/s    in 32s     

2024-08-30 16:40:37 (120 KB/s) - ‘delegated-apnic-latest.txt’ へ保存完了 [3861030/3861030]

...一部省略...

Getting https://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest.txt...
--2024-08-30 16:41:31--  https://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest.txt
ftp.ripe.net (ftp.ripe.net) をDNSに問いあわせています... 193.0.11.24
ftp.ripe.net (ftp.ripe.net)|193.0.11.24|:443 に接続しています... 接続しました。
HTTP による接続要求を送信しました、応答を待っています... 200 OK
長さ: 17339951 (17M) [text/plain]
‘delegated-ripencc-extended-latest.txt’ に保存中

delegated-ripencc-extended-lates 100%[==========================================================>]  16.54M   215KB/s    in 77s     

2024-08-30 16:42:50 (219 KB/s) - ‘delegated-ripencc-extended-latest.txt’ へ保存完了 [17339951/17339951]

Saved: csv/ipv4-all-2024-08-30.csv
```

(1) 作業日のディレクトリに生成されたファイル

```
~/Documents/public$ tree RIR/2024-08-30/
RIR/2024-08-30/
├── delegated-afrinic-latest.txt
├── delegated-apnic-latest.txt
├── delegated-arin-extended-latest.txt
├── delegated-lacnic-latest.txt
├── delegated-ripencc-extended-latest.txt
├── ipv4-1-apnic.csv
├── ipv4-2-afrinic.csv
├── ipv4-3-arin.csv
├── ipv4-4-lacnic.csv
├── ipv4-5-ripencc.csv
├── ipv4-allocated-afrinic.txt
├── ipv4-allocated-apnic.txt
├── ipv4-allocated-arin.txt
├── ipv4-allocated-lacnic.txt
└── ipv4-allocated-ripencc.txt
```

(2) インポート用CSVファイル

```
~/Documents/public/RIR$ tree csv/
csv/
└── ipv4-all-2024-08-30.csv
```

dockerコンテナ生成関連リソースとテーブル作成SQLなどについては下記にソースを配置しています  
[pipito-yukio / qiita-posts / python / Psycopg2](https://github.com/pipito-yukio/qiita-posts/tree/main/python/Psycopg2)

関連記事は下記Qiita投稿サイトをご覧ください。  
[@pipito-yukio(吉田 幸雄) psycopg2 バッチ処理に適したクエリーを作成する](https://qiita.com/pipito-yukio/items/ded82fd1018e378f4f1c)


