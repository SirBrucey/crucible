FROM powerdns/pdns-auth-49:latest
# Replaces the shipped config rather than adding to it: that one launches the
# sqlite backend, and its settings are unknown once gmysql is the one launched.
COPY pdns.conf /etc/powerdns/pdns.conf
