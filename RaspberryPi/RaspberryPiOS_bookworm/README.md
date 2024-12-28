### 下記 Qiita投稿で紹介したスクリプトのソースコード

それぞれ Raspberry Pi OS (bookworm) の NetworkManager用の接続設定ファイルを生成するスクリプトです。

スクリプトの内容に関しては以下の投稿をご覧ください。
+ (1) 有線ネットワーク用の接続設定ファイル生成スクリプト
[(Qiia@pipito-yukio) Raspberry Pi OS (bookworm) OSインストール時に固定IPアドレスを設定する](https://qiita.com/pipito-yukio/items/8538bc50ba06f4aa2f7a)
+ (2) Wi-Fiネットワーク用の接続設定ファイル生成スクリプト
[(Qiia@pipito-yukio) Raspberry Pi OS (bookworm) インストール時に固定IPアドレスを設定する(続編)](https://qiita.com/pipito-yukio/items/86233e0fff8466d9355e)


#### スクリプトの実行環境

+ OS: Ubuntu Desktop
※検証環境は 22-04 です。


#### ソースの構成

```
src/bin/raspi/
├── connections   # スクリプト実行後に生成されたNetworkManager用接続設定ファイル
│   ├── Wired_staticIp.connection
│   └── xxxxxx2g_staticIp.connection
├── gen_staticIP_ethernet_for_networkmanager.sh # 有線ネットワーク用の接続設定ファイル生成スクリプト
├── gen_staticIP_wifi_for_networkmanager.sh     # Wi-Fiネットワーク用の接続設定ファイル生成スクリプト
├── gen_wpa_supplicant_conf.sh                 # wpa_supplicant.conf を生成するスクリプト
└── wpa_supplicant.conf                        # SSIDとパスフレーズから生成された暗号化キーを含むファイル
```

#### 生成された接続設定ファイルのコピー先

SDカードをマウントしたときのルートファイル上の下記ディレクトリにコピーします。  
※コピーには sudo 権限が必要です。

 **```/etc/NetworkManager/system-connections```**


以上です。
