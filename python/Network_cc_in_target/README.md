[Qiita] 「IPアドレスの国コードを知ってセキュリティ対策に活用する」(仮称) で解説する予定のソースコード

ソースの構成
```
Network_cc_in_target/
├── README.md
├── requirements.txt
└── src
    ├── IpNetworkCC_in_hosts_with_csv.py
    ├── TestDetectCountryCode.py
    ├── conf
    │   └── db_conn.json
    ├── csv
    │   ├── ssh_auth_error_2024-06-10.csv
    │   └── ssh_auth_error_cc_match.csv
    ├── db
    │   └── pgdatabase.py
    ├── logs
    ├── mypy.ini
    └── output
```

