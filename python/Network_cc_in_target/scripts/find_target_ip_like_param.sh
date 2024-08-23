#!/bin/bash

# ./find_target_ip_like_param.sh ip_like param
# like-param: (example) 83.222.%

cat<<-EOF | psql -Udeveloper qiita_exampledb
SELECT
   ip_start,ip_count,country_code
FROM
   mainte2.RIR_ipv4_allocated
WHERE
   ip_start LIKE '${1}'
ORDER BY
 LPAD(SPLIT_PART(ip_start,'.',1), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',2), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',3), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',4), 3, '0');
EOF

