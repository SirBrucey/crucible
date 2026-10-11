FROM mariadb:11.4
COPY schema.sql /docker-entrypoint-initdb.d/01-schema.sql
RUN printf '[mariadbd]\nskip-ssl\n' > /etc/mysql/conf.d/plaintext.cnf
