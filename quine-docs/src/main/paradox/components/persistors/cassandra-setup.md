---
description: How to configure the Cassandra persistent data store for Quine
---
# Cassandra Persistor

The Cassandra persistor connects Quine to Cassandra and Cassandra compatible solutions like AstraDB and ScyllaDB.

## Quine Configuration

To use Cassandra as the persistence backend for Quine, you'll need to set the `quine.store` section to `type = cassandra` in the config.

@@snip [reference.conf]($quine$/src/test/resources/documented_cassandra_config.conf)

Where `endpoints` is a list of the address(es) of one or more Cassandra hosts in the cluster. If you need to specify a port other than 9042 (the default), you can use `host:portNum`.

Alternatively, you may specify the environment variable `CASSANDRA_ENDPOINTS` as a comma-separated list of hostnames, or host:ports, to be used if `endpoints` is not set in the config file.

### Quine Persistence Event Configuration

Related to the persistence store configuration, the persistence section of our config has settings related to when to save data.

``` ini
# configuration for which data to save about nodes and when to do so
persistence {
    # whether to save node journals. "true" uses more disk space and
    # enables more functionality, such as historical queries
    journal-enabled = true

    # one of [on-node-sleep, on-node-update, never]. When to save a
    # snapshot of a node's current state, including any DistinctId Standing
    # Queries registered on the node
    snapshot-schedule = on-node-sleep

    # whether only a single snapshot should be retained per-node. If false,
    # one snapshot will be saved at each timestamp against which a
    # historical query is made
    snapshot-singleton = false

    # when to save Standing Query partial result (only applies for the
    # `MultipleValues` mode -- `DistinctId` Standing Queries always save when
    # a node saves a snapshot, regardless of this setting)
    standing-query-schedule = on-node-sleep
}
```

For a hosted deployment, we recommend:

``` ini
persistence {
  journal-enabled = false
  snapshot-singleton = true
  standing-query-schedule = "on-node-sleep"
}
```

## Cassandra Authentication

Quine uses the [DataStax Java Driver for Apache Cassandra](https://mvnrepository.com/artifact/com.datastax.oss/java-driver-core/4.15.0) for connections. You can configure authentication by adding `datastax-java-driver` configuration to your local `quine.conf` file as described on the [Authentication page](https://docs.datastax.com/en/developer/java-driver/4.14/manual/core/authentication/).

For example, adding the following into a `quine.conf` file will set up basic authentication.

``` json
quine.store {
  # LOCAL Apache Cassandra Instance
  type = cassandra
}
datastax-java-driver {
  advanced {
    auth-provider {
      class = PlainTextAuthProvider
      username = cassandra
      password = cassandra
    }
  }
}
```

Then launch Quine with the following command line:

```shell
java -Dconfig.file=quine.conf -jar quine.jar
```

## Automatic Creation of Keyspace and Tables

Quine has settings in the Cassandra section of the config, `should-create-keyspace` and `should-create-tables`. When enabled, Quine automatically creates the keyspace and/or tables at startup if they don't already exist.

@@@ note { title=Note }
Auto creation of the keyspace and tables is included as a development convenience and should never be used in production.
@@@



## Cassandra Schema

``` sql
CREATE KEYSPACE quine WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
USE quine;

CREATE TABLE domain_graph_nodes (
    dgn_id bigint PRIMARY KEY,
    data blob
);

CREATE TABLE domain_index_events (
    quine_id blob,
    timestamp bigint,
    data blob,
    dgn_id bigint,
    PRIMARY KEY (quine_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'};

CREATE TABLE journals (
    quine_id blob,
    timestamp bigint,
    data blob,
    PRIMARY KEY (quine_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'};

CREATE TABLE meta_data (
    key text PRIMARY KEY,
    value blob
);

CREATE TABLE snapshots (
    quine_id blob,
    timestamp bigint,
    multipart_index int,
    data blob,
    multipart_count int,
    PRIMARY KEY (quine_id, timestamp, multipart_index)
) WITH CLUSTERING ORDER BY (timestamp DESC, multipart_index ASC);

CREATE TABLE standing_queries (
    query_id uuid PRIMARY KEY,
    queries blob
);

CREATE TABLE standing_query_states (
    quine_id blob,
    standing_query_id uuid,
    standing_query_part_id uuid,
    data blob,
    PRIMARY KEY (quine_id, standing_query_id, standing_query_part_id)
) WITH CLUSTERING ORDER BY (standing_query_id ASC, standing_query_part_id ASC);
```

## AstraDB Configuration

Astra DB is a fully Cassandra compatible and serverless DbaaS that simplifies the development and deployment of high-growth applications on AWS.

Set the following in the `quine.conf` file to connect AstraDB:

``` ini
quine.store {
  # store data in an Apache Cassandra instance
  type = cassandra
  # the keyspace to use
  keyspace = quine
  should-create-keyspace = false
  should-create-tables = true
  replication-factor = 3
  write-consistency = LOCAL_QUORUM
  read-consistency = LOCAL_QUORUM
  local-datacenter = ${ASTRA_DB_REGION}
  write-timeout = "10s"
  read-timeout = "10s"
}
datastax-java-driver {
  advanced {
    auth-provider {
      class = PlainTextAuthProvider
      username = "token"
      password = "${ASTRA_DB_APP_TOKEN}"
    }
  }
  basic {
    cloud {
      secure-connect-bundle = "${SECURE-CONNECT-BUNDLE}.zip"
    }
  }
}
```

### Astra-Specific Settings

`type = cassandra` - Use the Cassandra persistor to connect to AstraDB

`should-create-keyspace = false` - Remember keyspaces can only be created in Astra via the dashboard.

`replication-factor = 3` - Defaults to 1 if not set.

`write-consistency = LOCAL_QUORUM` - Minimum consistency level required by Astra.

`read-consistency = LOCAL_QUORUM` - Minimum consistency level required by Astra.

`local-datacenter = "us-east1"` - Set your Astra DB cloud region as the local DC.

`username = "token"` - Leave it as the literal word "token."

`password` - A valid token for an Astra DB cluster.

`secure-connect-bundle` - A valid, local file location of a downloaded Astra secure connect bundle. The driver gets the Astra DB hostname from the secure bundle, so there is no need to specify endpoints separately.

## ScyllaDB Configuration

ScyllaDB is an open-source distributed NoSQL wide-column data store. It was designed to be compatible with Apache Cassandra while achieving significantly higher throughput and lower latencies.

Set the following in the `quine.conf` file to connect ScyllaDB:

``` ini
quine.store {
  type = cassandra
  endpoints = ["cassandraHostAddress"]
}
```
