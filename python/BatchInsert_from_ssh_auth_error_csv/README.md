[Qiita] 「不正アクセスしたIPアドレスのランキングをデータベースで管理する」で解説したソースコードとテスト用CSVファイル

ソースの構成
```
BatchInsert_from_ssh_auth_error_csv/
├── README.md
├── docker
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── initdb
│       ├── 10_createdb.sql
│       └── 11_createtable.sql
├── scripts
│   ├── dkr_exec_show_unauth_accessed_ip.sh
│   └── show_unauth_accessed_ip_for_morethan_3days.sh
└── src
    ├── BatchInsert_mainte2_daterange_csv.py
    ├── BatchInsert_with_csv.py
    ├── conf
    │   └── db_conn.json
    ├── csv
    │   ├── ssh_auth_error_2024-06-10.csv
    │   ├── ssh_auth_error_2024-06-11.csv
    │   ├── ssh_auth_error_2024-06-12.csv
    │   ├── ssh_auth_error_2024-06-13.csv
    │   ├── ssh_auth_error_2024-06-14.csv
    │   ├── ssh_auth_error_2024-06-15.csv
    │   └── ssh_auth_error_2024-06-16.csv
    └── db
        └── pgdatabase.py
```

約3ヶ月分のCSVファイルは下記GitHubサイトで公開しています。  
[期間] 2024-06-10 〜 2024-09-08  
[GitHub pipito-yukio / ubuntu_server_tools / csv /](https://github.com/pipito-yukio/ubuntu_server_tools/tree/main/csv)

