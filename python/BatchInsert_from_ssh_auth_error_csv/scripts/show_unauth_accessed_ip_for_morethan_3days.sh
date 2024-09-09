#!/bin/bash

# dockerコンテナ内の psqlでクエリを実行するシェルスクリプト
#  ※dockerコンテナ内で実行する必要がある
# 検索期間で出現回数が30回を超え、かつ連続して3日以上不正アフクセスしたIPアドレスを出力
# (例) 2024-06-01 2024-06-30

echo "[検索期間] $1 〜 $2"

cat<<-EOF | psql -Udeveloper qiita_exampledb --tuples-only
-- unauth_ip_addr と ssh_auth_errorの結合 
WITH t_ip_joined AS(
  SELECT
     log_date, ip_addr, appear_count
  FROM
     mainte2.ssh_auth_error sae
     INNER JOIN mainte2.unauth_ip_addr ip_t
     ON sae.ip_id = ip_t.id
),
-- 1日あたりの不正アクセス回数が30回を超えるIPアドレス
ip_appear_count_over30 AS(
  SELECT ip_addr,count(ip_addr) as ip_cnt_in_days
FROM
  (SELECT log_date, ip_addr, appear_count FROM t_ip_joined WHERE appear_count > 30)
WHERE
   log_date BETWEEN '${1}' AND '${2}'
GROUP BY ip_addr
),
-- 3日以上連続して不正アクセスしてきたIPアドレス
access_morethan_3days_ip AS(
  SELECT ip_addr FROM ip_appear_count_over30 WHERE ip_cnt_in_days >= 3
)
-- 検索期間で3日以上連続して不正アクセスしてきたIPアドレスをログ収集日を含めて出力
-- ※IPアドレスをソート用に加工した文字列でソートして出力
SELECT
   log_date, ip_addr, appear_count
FROM
   t_ip_joined
WHERE
   log_date BETWEEN '${1}' AND '${2}'
   AND
   ip_addr in (SELECT * FROM access_morethan_3days_ip)
-- IPアドレス(ドットで分割したそれぞれの数値が3桁未満なら3桁になるまで'0'を付加)順, ログ日付順
-- ソートキー用の加工例: '79.110.62.14' -> '079.110.062.014'
ORDER BY
    lpad(split_part(ip_addr,'.',1),3,'0')
 || lpad(split_part(ip_addr,'.',2),3,'0')
 || lpad(split_part(ip_addr,'.',3),3,'0')
 || lpad(split_part(ip_addr,'.',4),3,'0'), log_date;
EOF

