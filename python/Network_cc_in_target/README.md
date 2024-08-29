[Qiita] 「不正アクセスしてきたホストの国コードを知ってセキュリティ対策に活用する」で解説したソースコードと生成ファイル

ソースの構成
```
Network_cc_in_target/
├── README.md
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
{
  "output-dir": "~/Documents/qiita/sql/batch",
  "query": {
    "match-networks-dir": "~/Documents/qiita/match_networks"
  }
}


dockerコンテナ生成関連リソースとテーブル作成SQLなどについては下記にソースを配置しています
[pipito-yukio / qiita-posts / python / Psycopg2](https://github.com/pipito-yukio/qiita-posts/tree/main/python/Psycopg2)

関連記事は下記Qiita投稿サイトをご覧ください。
[@pipito-yukio(吉田 幸雄) psycopg2 バッチ処理に適したクエリーを作成する](https://qiita.com/pipito-yukio/items/ded82fd1018e378f4f1c)


