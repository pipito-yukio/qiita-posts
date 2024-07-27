[Qiita] 「psycopg2 バッチ処理に適したクエリーを作成する」で解説したソースコードです

ソースの構成

```
.
├── README.md
└── src
    ├── docker                                       # dockerコンテナ作成リソース
    │   ├── .env
    │   ├── Dockerfile
    │   ├── docker-compose.yml
    │   └── initdb
    │       └── 10_createdb.sql                      # データベース作成SQL
    ├── python_script
    │   ├── InsertBatches_ssh_auth_error_csvfiles.py # 複数のCSVファイルからssh_auth_errorテーブルに一括登録するスクリプト 
    │   ├── InsertValues_unauth_ip_addr_csvfiles.py  # 複数のCSVファイルからunauth_ip_addrテーブルに一括登録するスクリプト 
    │   ├── TestBulkInsert_ssh_auth_error.py         # ssh_auth_errorテーブルの一括登録スクリプト 
    │   ├── TestBulkInsert_unauth_ip_addr.py         # ssh_unauth_ip_addrテーブルの一括登録スクリプト 
    │   ├── TestExists_ssh_auth_error.py             # ssh_auth_errorテーブル 存在チェックスクリプト
    │   ├── TestExists_unauth_ip_addr.py             # unauth_ip_addrテーブル 存在チェックスクリプト
    │   ├── conf
    │   │   └── db_conn.json                         # データベース接続設定ファイル
    │   ├── csv                                      # テスト用CSV
    │   │   └── ssh_auth_error_2024-06-18.csv
    │   ├── dao
    │   │   ├── __init__.py
    │   │   ├── ssh_auth_error.py                    # ssh_auth_errorテーブル登録関数定義モジュール
    │   │   └── unauth_ip_addr.py                    # unauth_ip_addrテーブル登録関数定義モジュール
    │   └── db
    │       ├── __init__.py
    │       └── pgdatabase.py                        # PostgreSQLサーバー接続モジュール
    ├── requrements.txt
    └── sql
        └── mainte2_createtable.sql                  # テーブル生成SQL 
```
