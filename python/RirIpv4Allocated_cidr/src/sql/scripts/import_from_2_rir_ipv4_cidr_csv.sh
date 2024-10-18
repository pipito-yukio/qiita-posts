#!/bin/bash

# https://stackoverflow.com/questions/34736762/script-to-automat-import-of-csv-into-postgresql
#   Script to automat import of CSV into PostgreSQL

# PK制約をドロップ
psql -Udeveloper -d qiita_exampledb -c \
"ALTER TABLE mainte2.RIR_ipv4_allocated_cidr
 DROP CONSTRAINT pk_RIR_ipv4_allocated_cidr;"


sleep 1

# データインポート
psql -Udeveloper -d qiita_exampledb -c \
"\copy mainte2.RIR_ipv4_allocated_cidr FROM 
'/home/qiita/data/sql/qiita_example/mainte2/csv/RIR/${1}' 
DELIMITER ',' CSV HEADER;"

sleep 2

# PK制約を戻す
psql -Udeveloper -d qiita_exampledb -c \
"ALTER TABLE mainte2.RIR_ipv4_allocated_cidr
 ADD CONSTRAINT pk_RIR_ipv4_allocated_cidr PRIMARY KEY (network_addr);"

