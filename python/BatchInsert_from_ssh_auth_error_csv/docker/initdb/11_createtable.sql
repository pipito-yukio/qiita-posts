\connect qiita_exampledb

-- Qiita投稿用テスト用スキーマ
CREATE SCHEMA mainte2;

-- 不正アクセスIPアドレス管理マスタ
CREATE TABLE mainte2.unauth_ip_addr(
   id INTEGER NOT NULL,
   ip_addr VARCHAR(15) NOT NULL,
   reg_date DATE NOT NULL,
   country_code CHAR(2)
);
CREATE SEQUENCE mainte2.ip_addr_id OWNED BY mainte2.unauth_ip_addr.id;
ALTER TABLE mainte2.unauth_ip_addr ALTER id SET DEFAULT nextval('mainte2.ip_addr_id');
ALTER TABLE mainte2.unauth_ip_addr ADD CONSTRAINT pk_unauth_ip_addr PRIMARY KEY (id);
-- IPアドレスは重複なし
CREATE UNIQUE INDEX idx_ip_addr ON mainte2.unauth_ip_addr(ip_addr);

-- 不正アクセスカウンターテープル
CREATE TABLE mainte2.ssh_auth_error(
   log_date date NOT NULL,
   ip_id INTEGER,
   appear_count INTEGER NOT NULL
);
ALTER TABLE mainte2.ssh_auth_error ADD CONSTRAINT pk_ssh_auth_error
   PRIMARY KEY (log_date, ip_id);
ALTER TABLE mainte2.ssh_auth_error ADD CONSTRAINT fk_ssh_auth_error
   FOREIGN KEY (ip_id) REFERENCES mainte2.unauth_ip_addr (id);

ALTER SCHEMA mainte2 OWNER TO developer;
ALTER TABLE mainte2.unauth_ip_addr OWNER TO developer;
ALTER TABLE mainte2.ssh_auth_error OWNER TO developer;
