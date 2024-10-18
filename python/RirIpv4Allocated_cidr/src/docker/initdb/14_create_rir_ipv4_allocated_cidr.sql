\connect qiita_exampledb

-- Qiita投稿用テスト用スキーマ
CREATE SCHEMA mainte2;

-- 古いテーブルが存在したらドロップする
DROP TABLE IF EXISTS mainte2.RIR_ipv4_allocated_cidr CASCADE;
DROP TABLE IF EXISTS mainte2.RIR_registory_mst;

-- レジストリ名テーブル
-- name: {afrinic,apnic,arin,iana,lacnic,ripencc}
-- https://www.apnic.net/about-apnic/corporate-documents/documents/
--     resource-guidelines/rir-statistics-exchange-format/
CREATE TABLE mainte2.RIR_registory_mst(
   id SMALLINT PRIMARY KEY,
   name VARCHAR(8) NOT NULL
);

INSERT INTO mainte2.RIR_registory_mst(id, name) VALUES 
   (1,'apnic')
  ,(2,'afrinic')
  ,(3,'arin')
  ,(4,'lacnic')
  ,(5,'ripencc')
  ,(6,'iana');

-- APNICで公開している各国に割り当てているIPアドレス情報からipv4アドレスのみを抽出したマスタテーブル
CREATE TABLE mainte2.RIR_ipv4_allocated_cidr(
   network_addr CIDR NOT NULL,
   country_code CHAR(2) NOT NULL,
   allocated_date DATE NOT NULL,
   registry_id SMALLINT NOT NULL
);

ALTER TABLE mainte2.RIR_ipv4_allocated_cidr ADD CONSTRAINT pk_RIR_ipv4_allocated_cidr
  PRIMARY KEY (network_addr);
ALTER TABLE mainte2.RIR_ipv4_allocated_cidr ADD CONSTRAINT fk_RIR_ipv4_allocated_cidr_registry
  FOREIGN KEY (registry_id) REFERENCES mainte2.RIR_registory_mst (id);

ALTER TABLE mainte2.RIR_registory_mst OWNER TO developer;
ALTER TABLE mainte2.RIR_ipv4_allocated_cidr OWNER TO developer;

