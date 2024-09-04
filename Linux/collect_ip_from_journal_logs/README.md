[Qiita] 「ジャーナルログからSSHで不正アクセスするホストのIPアドレスを収集する」で解説したソースコードとサンプルファイル

ソースの構成

collect_ip_from_journal_logs/
├── README.md
├── bin
│   └── ssh_auth_error.sh
└── python
    ├── ExportCSV_with_autherrorlog.py
    ├── csv
    │   └── ssh_auth_error_2024-07-09.csv
    └── error_logs
        └── AuthFail_ssh_2024-07-09.log    # サンプルSSHエラーログファイル

