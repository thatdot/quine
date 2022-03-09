# Cassandra Persistor Deployment

## Introduction

Instructions to install Cassandra from `.deb` or `.rpm` are available at: @link:[https://cassandra.apache.org/download/](https://cassandra.apache.org/download/){ open=new }

## Disk Storage Configuration

Data managed by Cassandra is stored locally on disk at: `/var/lib/cassandra`.

On EC2 instances with attached ephemeral storage (such as an @link:[I3 type](https://aws.amazon.com/ec2/instance-types/i3/){ open=new }, or an e.g. M5 instance with a "d" in the name), you may want to mount the local SSD drive there.

### **Example setup:**
Format and mount the data volume for Cassandra to use at `/var/lib/cassandra`

`sudo mkfs.xfs -L cassandra /dev/nvme1n1  # Or whatever the name of the attached ephemeral storage device is`

`sudo mkdir /var/lib/cassandra`

And then add the following line to `/etc/fstab`

`LABEL=cassandra /var/lib/cassandra xfs defaults,nofail,noatime,noquota 0 2`

`sudo mount /var/lib/cassandra`

`sudo chown cassandra:cassandra /var/lib/cassandra`

## Network Configuration

Configuration is in `/etc/cassandra` (or `/etc/cassandra/conf` on Amazon Linux 2).

The main config file is @link:[cassandra.yml](https://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html){ open=new }

The minimum required changes to this file required to deploy a Cassandra cluster are: comment out the `rpc_address `and `listen_address` settings in this file, and set the seed address setting to the address of one or more hosts in the cluster. Start the seed node(s) first, and then bring up successive nodes one at a time.

The default `cassandra.yml` has a couple addresses set to `localhost: rpc_address`, and `listen_address`.

* `rpc_address` is the address which the application uses to communicate with Cassandra (default port 9042).
* `listen_address` is the address where Cassandra will listen for communication from other members of the Cassandra cluster (default port 7000). You can leave this alone if you're only running a single Cassandra instance.

If these settings are commented out, Cassandra will use the value of `java.net.InetAddress.getLocalHost()`. On EC2 instances, this returns the internal IP of the instance (e.g. 10.0.xxx.xxx), which works well for this case. Alternatively, you can explicitly set them to an address, or set their `_interface`-suffixed variants if you wish to specify network interface rather than address.

There is also `broadcast_address` / `broadcast_interface` if for some reason you need the address the cluster member listens on (binds to) to be different that the address by which it is known to other cluster members, e.g. behind a NAT. This setting defaults to use the value of `listen_address`.

### Clustering

If you're running a cluster of Cassandra instances, you'll need to populate the `seeds` setting with the address of one or more Cassandra servers to use as seed nodes, for the members to use to discover the cluster.

For more info, refer to: @link:[https://docs.datastax.com/en/dse/6.8/dse-admin/datastax_enterprise/production/seedNodesForSingleDC.html](https://docs.datastax.com/en/dse/6.8/dse-admin/datastax_enterprise/production/seedNodesForSingleDC.html){ open=new } and: @link:[https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/initialize/initSingleDS.html](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/initialize/initSingleDS.html){ open=new }.

## Performance Optimization

There are some recommended `systctl` and `ulimit` settings you adjust on the OS - see @link:[https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/install/installRecommendSettings.html](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/install/installRecommendSettings.html){ open=new }

For our testing, we followed the recommendations on @link:[https://docs.datastax.com/en/docker/doc/docker/dockerRecommendedSettings.html#dockerRecommendedSettings__userRes](https://docs.datastax.com/en/docker/doc/docker/dockerRecommendedSettings.html#dockerRecommendedSettings__userRes){ open=new } (as we were deploying on ECS)

Ulimits were set, with soft and hard limits, respectively, to:

`nofile=100000:100000`

`nproc=32768:100000`

`memlock=-1:-1`

In `/etc/sysctl.d/50-cassandra.conf`, we added:

`vm.max_map_count=104857`

Then we ran `sysctl --system` to make it take effect, and the `IPC_LOCK` capability was added.

See: @link:[https://aws.amazon.com/blogs/big-data/best-practices-for-running-apache-cassandra-on-amazon-ec2/](https://aws.amazon.com/blogs/big-data/best-practices-for-running-apache-cassandra-on-amazon-ec2/){ open=new } and @link:[https://docs.datastax.com/en/dse-planning/doc/planning/planningEC2.html#GuidelinesforEC2productionclusters](https://docs.datastax.com/en/dse-planning/doc/planning/planningEC2.html#GuidelinesforEC2productionclusters){ open=new } for more recommendations for running Cassandra on AWS.

##  Quine Configuration For Use With Cassandra

To use Cassandra as the persistence backend for Quine, at a minimum, you’ll need to set in config:

```
thatdot.quine.store {
  type = cassandra
  endpoints = ["cassandraHostAddress"]
}
```

Where `endpoints` is a list of strings that are the address(es) of one or more Cassandra hosts in the cluster. If you need to specify a port other than 9042 (the default), you can use “host:portNum” strings.

Alternatively, you may specify the environment variable `CASSANDRA_ENDPOINTS` (as a comma-separated list of hostnames, or host:ports), which takes precedence over the above config.

###  Default Cassandra Config Reference

The full range of config options available for this store section is (with default values) is as follows:

@@snip [reference.conf]($quine$/src/test/resources/documented_cassandra_config.conf)

#### Actual config used:

We’ve used the following config for that section. See @link:[this doc for available consistency levels on Cassandra](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/dml/dmlConfigConsistency.html){ open=new }.

For this performance testing, we’ve chosen to optimize for performance at the expense of cluster consistency. Because we have a `decline-sleep-when-write-within` setting defaulted to 100ms, meaning we shouldn’t require immediate reading back of data that’s written, we felt this was an acceptable risk for this case. \

```
store {
  type = cassandra
  # We use AWS CloudMap to register an internal DNS entry for Cassandra seed nodes:
  endpoints = ["seed.cassandra"]
  read-consistency = ONE
  write-consistency = ANY
}
```

### Quine Persistence Event Configuration

Relatedly, in the persistence section of our config, we have settings related to when to save data. The default values are given here:

```
   # configuration for which data to save about nodes and when to do so
    persistence {
      # whether to save node journals. "true" uses more disk space and
      # enables more functionality, such as historical queries
      journal-enabled = true

      # one of [on-node-sleep, on-node-update, never]. When to save a
      # snapshot of a node's current state, including any SingleId Standing
      # Queries registered on the node
      snapshot-schedule = on-node-sleep

      # whether only a single snapshot should be retained per-node. If false,
      # one snapshot will be saved at each timestamp against which a
      # historical query is made
      snapshot-singleton = false

      # when to save Standing Query partial result (only applies for the
      # `MultipleValues` mode -- `SingleId` Standing Queries always save when
      # a node saves a snapshot, regardless of this setting)
      standing-query-schedule = on-node-sleep
    }
```

For a hosted deployment, we’ve had those set as follows:

```
persistence {
  journal-enabled = false
  snapshot-singleton = true
  standing-query-schedule = "on-node-sleep"
}
```

## Automatic Creation of Keyspace and Tables

We have a couple settings in the Cassandra section of the config, `should-create-keyspace `and` should-create-tables`, that when enabled (they default to true), will have Quine automatically create the keyspace and/or tables at startup if they don’t already exist. However, in the clustered case, because Cassandra doesn’t currently support concurrent CREATE TABLE IF NOT EXISTS statements for the same table (see @link:[CASSANDRA-10699](https://issues.apache.org/jira/browse/CASSANDRA-10699){ open=new }), this can lead to exceptions being thrown on the client at startup of the form: `org.apache.cassandra.exceptions.ConfigurationException: Column family ID mismatch (found e9daecc0-15b7-11ec-a406-6d2c86545d91; expected e9d98d30-15b7-11ec-a406-6d2c86545d91)`

You can restart the Quine cluster if this happens. To forestall this, you could boot one node first and let it create those tables, and then have the rest of the cluster join.

Or you could create them ahead of time by running the following CQL (feel free to customize the keyspace settings as desired):

### Cassandra Schema Used:

```
CREATE KEYSPACE thatdot WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE thatdot.journals (
    quine_id blob,
    timestamp bigint,
    data blob,
    PRIMARY KEY (quine_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'};

CREATE TABLE thatdot.meta_data (
    key text PRIMARY KEY,
    value blob
);

CREATE TABLE thatdot.snapshots (
    quine_id blob,
    timestamp bigint,
    data blob,
    PRIMARY KEY (quine_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE thatdot.standing_queries (
    query_id uuid PRIMARY KEY,
    queries blob
);

CREATE TABLE thatdot.standing_query_states (
    quine_id blob,
    standing_query_id uuid,
    standing_query_part_id uuid,
    data blob,
    PRIMARY KEY (quine_id, standing_query_id, standing_query_part_id)
) WITH CLUSTERING ORDER BY (standing_query_id ASC, standing_query_part_id ASC);
```
