-- The tables PowerDNS reads to answer a query, and the one zone this example
-- serves. Its schema has four more, for DNSSEC, TSIG, zone transfers and its
-- API, none of which this fleet uses.
--
-- Baked into the image because the fleet is brought up with no volumes: a
-- service arrives with whatever its image holds and nothing else.

CREATE DATABASE IF NOT EXISTS pdns;
USE pdns;

CREATE TABLE domains (
  id                    INT AUTO_INCREMENT,
  name                  VARCHAR(255) NOT NULL,
  master                VARCHAR(128) DEFAULT NULL,
  last_check            INT DEFAULT NULL,
  type                  VARCHAR(8) NOT NULL,
  notified_serial       INT UNSIGNED DEFAULT NULL,
  account               VARCHAR(40) CHARACTER SET 'utf8' DEFAULT NULL,
  options               VARCHAR(64000) DEFAULT NULL,
  catalog               VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id)
) Engine=InnoDB CHARACTER SET 'latin1';

CREATE UNIQUE INDEX name_index ON domains(name);

CREATE TABLE records (
  id                    BIGINT AUTO_INCREMENT,
  domain_id             INT DEFAULT NULL,
  name                  VARCHAR(255) DEFAULT NULL,
  type                  VARCHAR(10) DEFAULT NULL,
  content               VARCHAR(64000) DEFAULT NULL,
  ttl                   INT DEFAULT NULL,
  prio                  INT DEFAULT NULL,
  disabled              TINYINT(1) DEFAULT 0,
  ordername             VARCHAR(255) BINARY DEFAULT NULL,
  auth                  TINYINT(1) DEFAULT 1,
  PRIMARY KEY (id)
) Engine=InnoDB CHARACTER SET 'latin1';

CREATE INDEX nametype_index ON records(name,type);
CREATE INDEX domain_id ON records(domain_id);

-- Consulted on every lookup, before the records are read, so a zone whose
-- metadata cannot be listed is not answered for at all.
CREATE TABLE domainmetadata (
  id                    INT AUTO_INCREMENT,
  domain_id             INT NOT NULL,
  kind                  VARCHAR(32),
  content               TEXT,
  PRIMARY KEY (id)
) Engine=InnoDB CHARACTER SET 'latin1';

CREATE INDEX domainmetadata_idx ON domainmetadata (domain_id, kind);

-- The zone the scenario writes into, with the apex records a zone needs before
-- it can be answered for at all.
INSERT INTO domains (name, type) VALUES ('example.test', 'NATIVE');
INSERT INTO records (domain_id, name, type, content, ttl)
SELECT id, 'example.test', 'SOA', 'ns.example.test hostmaster.example.test 1 10380 3600 604800 3600', 60
FROM domains WHERE name = 'example.test';
INSERT INTO records (domain_id, name, type, content, ttl)
SELECT id, 'example.test', 'NS', 'ns.example.test', 60
FROM domains WHERE name = 'example.test';
