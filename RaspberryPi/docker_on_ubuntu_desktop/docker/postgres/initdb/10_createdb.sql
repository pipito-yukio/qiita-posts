CREATE ROLE developer WITH LOGIN PASSWORD 'yourpasswd';
ALTER ROLE developer WITH SUPERUSER;
CREATE DATABASE qiita_exampledb WITH OWNER=developer ENCODING='UTF-8' LC_COLLATE='ja_JP.UTF-8' LC_CTYPE='ja_JP.UTF-8' TEMPLATE=template0;
GRANT ALL PRIVILEGES ON DATABASE qiita_exampledb TO developer;
