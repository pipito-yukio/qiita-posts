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
    ├── BatchInsert_mainte2_daterange_csv.py  # 指定したディレクトリ内の期間ファイルを一括登録するスクリプト
    ├── BatchInsert_with_csv.py               # 指定したCSVファイルを一括登録するスクリプト
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

#### BatchInsert_mainte2_daterange_csv.py の使い方  
※詳しくはソースをご覧になって下さい。

```bash
(py_psycopg2) $ python BatchInsert_mainte2_daterange_csv.py \
> --csv-dir ~/Documents/qiita_example/csv --from-date 2024-06-10 --to-date 2024-06-16
INFO from-date: 2024-06-10, to-date: 2024-06-16
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-10.csv: 12
INFO exists_ip_dict.size: 0
INFO reg_ip_datas.size: 12
INFO registered_ip_ids.size: 12
INFO Register ssh_auth_error_list.size: 12
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-11.csv: 52
INFO exists_ip_dict.size: 0
INFO reg_ip_datas.size: 52
INFO registered_ip_ids.size: 52
INFO Register ssh_auth_error_list.size: 52
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-12.csv: 49
INFO exists_ip_dict.size: 0
INFO reg_ip_datas.size: 49
INFO registered_ip_ids.size: 49
INFO Register ssh_auth_error_list.size: 49
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-13.csv: 43
INFO exists_ip_dict.size: 3
INFO Registered_count: 3
INFO reg_ip_datas.size: 40
INFO registered_ip_ids.size: 40
INFO Register ssh_auth_error_list.size: 43
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-14.csv: 24
INFO exists_ip_dict.size: 3
INFO Registered_count: 3
INFO reg_ip_datas.size: 21
INFO registered_ip_ids.size: 21
INFO Register ssh_auth_error_list.size: 24
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-15.csv: 26
INFO exists_ip_dict.size: 3
INFO Registered_count: 3
INFO reg_ip_datas.size: 23
INFO registered_ip_ids.size: 23
INFO Register ssh_auth_error_list.size: 26
INFO /home/qiita/Documents/qiita_example/csv/ssh_auth_error_2024-06-16.csv: 17
INFO exists_ip_dict.size: 2
INFO Registered_count: 2
INFO reg_ip_datas.size: 15
INFO registered_ip_ids.size: 15
INFO Register ssh_auth_error_list.size: 17
```

#### 実行結果の確認
dockerコンテナ上で実行

```bash
$ docker exec -it postgres-qiita bin/bash
13b8e6510d1f:/# echo "SELECT MAX(reg_date),COUNT(*) FROM mainte2.unauth_ip_addr;" | psql -Udeveloper qiita_exampledb
    max     | count 
------------+-------
 2024-06-16 |   212
(1 row)

13b8e6510d1f:/# echo "SELECT MAX(log_date),COUNT(*) FROM mainte2.ssh_auth_error;" | psql -Udeveloper qiita_exampledb
    max     | count 
------------+-------
 2024-06-16 |   223
(1 row)

```

約3ヶ月分のCSVファイルは下記GitHubサイトで公開しています。  
[期間] 2024-06-10 〜 2024-09-08  
[GitHub pipito-yukio / ubuntu_server_tools / csv /](https://github.com/pipito-yukio/ubuntu_server_tools/tree/main/csv)

