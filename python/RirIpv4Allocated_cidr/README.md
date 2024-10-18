[Qiita] 「地域インターネットレジストリデータをネットワークアドレスに変換する」で解説したソースとテスト用CSVファイル

**```rir_download_to_csv.sh```** の使い方は下記リポジトリに実行例を掲載しています。

[GitHub@pipito-yukio qiita-posts / python / Network_cc_in_target](https://github.com/pipito-yukio/qiita-posts/tree/main/python/Network_cc_in_target)


ソースの構成
```
RirIpv4Allocated_cidr/
├── README.md
├── bin
│   └── rir_download_to_csv.sh # 最新の地域インターネットレジストリデータからインポート用CSVファイル生成するシェルスクリプト
└── src
    ├── docker
    │   ├── Dockerfile
    │   ├── docker-compose.yml
    │   └── initdb
    │       ├── 10_createdb.sql
    │       └── 14_create_rir_ipv4_allocated_cidr.sql
    ├── python_project
    │   ├── RirIpv4Allocated_to_cidr_csv.py       # IPアドレス情報をネットワークアドレス(CIDR形式)に変換
    │   ├── TestDetectCountryCode_in_rir_cidr.py  # ネットワークアドレス・国コード検索
    │   ├── conf
    │   │   └── db_conn.json
    │   ├── csv
    │   │   └── ipv4-all-2024-10-16.csv           # 2024-10-16時点のRIRデータのオリジナルCSVファイル (変換前)
    │   └── mypy.ini
    ├── requirements.txt
    └── sql
        └── scripts
            └── import_from_2_rir_ipv4_cidr_csv.sh # 変換後のCSVインポートシェルスクリプト
```


