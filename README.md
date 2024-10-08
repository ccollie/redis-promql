# ValkeyMetrics

ValkeyMetrics is a module providing a [Prometheus](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata)-like API over timeseries data.
Add your data and query it using [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).

Currently supported are [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) and [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors),
as well as basic [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) lookups.

Substantially based on the [VictoriaMetrics](https://victoriametrics.com) project, this module is a work-in-progress and not yet ready for production use.

## Features
- In-memory storage for time series data
- Configurable data retention period
- Supports [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) queries
- Supports [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) and [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors)
- Supports [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) like lookups
- Exposes an API similar to the Prometheus HTTP-API
- Over 200 supported [functions](https://docs.victoriametrics.com/MetricsQL.html#metricsql-functions) (Label, Aggregation, Rollup and Transformation)

## Caveats
- Is highly experimental and not yet ready for production use
- The library does up-front query optimization and caching, so one-off ad-hoc queries are not as fast as repeated queries. These behaviours will be made configurable in future releases.

## Quick Example

Here we'll create a time series representing sensor temperature measurements.
After you create the time series, you can send temperature measurements.
Then you can query the data for a time range on some aggregation rule.

### With `redis-cli`
```sh
$ redis-cli
127.0.0.1:6379> VKM.CREATE temperature:3:east temperature{area_id="32",sensor_id="1",region="east"} RETENTION 2h
OK
127.0.0.1:6379> VKM.CREATE temperature:3:west temperature{area_id="32",sensor_id="1",region="west"} RETENTION 2h
OK
127.0.0.1:6379> VKM.ADD temperature:3:east 1548149181 30
OK
127.0.0.1:6379> VKM.ADD temperature:3:west 1548149191 42
OK 
127.0.0.1:6379>  VKM.QUERY-RANGE "avg(temperature) by(area_id)" START 1548149180 END 1548149210   
```

## Tests

The module includes a basic set of unit tests and integration tests.

**Unit tests**

To run all unit tests, follow these steps:

    $ cargo test


**Integration tests**

```bash
RLTest --module ./target/debug/libredis_promql.so --module-args
```

#### MacOSX
```bash
RLTest --module ./target/debug/libredis_promql.dylib --module-args
```

## Commands

Command names and option names are case-insensitive.

#### A note about keys
Since the general use-case for this module is querying across timeseries, it is a best practice to group related timeseries 
using "hash tags" in the key. This allows for more efficient querying across related timeseries. For example, if your
metrics are generally grouped by environment, you could use a key like 
`latency:api:{dev}` and `latency:frontend:{staging}`. If you are more likely to group by service, you could use
`latency:{api}:dev` and `latency:{frontend}:staging`.


https://tech.loveholidays.com/redis-cluster-multi-key-command-optimisation-with-hash-tags-8a2bd7ce12de


**VM.QUERY** evaluates an instant query at a single point in time.

```
VM.QUERY query [TIME timestamp|rfc3339|+|*] [ROUNDING number]
```

#### Options

- **query**: Prometheus expression query string.
- **TIME**: evaluation timestamp. Optional. If not specified, use current server time.
- **ROUNDING**: Optional number of decimal places to round values.

#### Return

 TODO

#### Error

Return an error reply in the following cases:

- Query syntax errors.
- A metric references in the query is not found.
- Resource exhaustion / query timeout.

#### Examples

```
VM.QUERY "sum(rate(process_io_storage_written_bytes_total)) by (job)" TIME 1587396550
```

### VM.QUERY-RANGE

#### Syntax

```
VM.QUERY-RANGE query [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*] [STEP duration|number] [ROUNDING number]
```

**VM.QUERY-RANGE** evaluates an expression query over a range of time.

#### Options

- **query**: Prometheus expression query string.
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.
- **STEP**: Query resolution step width in duration format or float number of seconds.
- **ROUNDING**: Optional number of decimal places to round values.

#### Return

- TODO

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
VM.QUERY-RANGE "sum(rate(rows_inserted_total[5m])) by (type,accountID) > 0" START 1587396550 END 1587396550 STEP 1m
```

### VM.DELETE-RANGE

#### Syntax

```
VM.DELETE-RANGE selector.. [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VM.DELETE-RANGE** deletes data for a selection of series in a time range. The timeseries itself is not deleted even if all samples are removed.

#### Options

- **selector**: one or more PromQL series selector.
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

- the number of samples deleted.

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
VM.DELETE-RANGE "http_requests{env='staging', status='200'}" START 1587396550 END 1587396550
```


### VM.SERIES

#### Syntax

```
VM.SERIES FILTER filterExpr... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VM.SERIES** returns the list of time series that match a certain label set.

#### Options

- **filterExpr**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the query result consists of a list of objects that contain the label name/value pairs which identify 
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples

The following example returns all series that match either of the selectors `up` or `process_start_time_seconds{job="prometheus"}`:

```
VM.SERIES FILTER up process_start_time_seconds{job="prometheus"}
``` 
```json
{
   "status" : "success",
   "data" : [
      {
         "__name__" : "up",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      },
      {
         "__name__" : "up",
         "job" : "node",
         "instance" : "localhost:9091"
      },
      {
         "__name__" : "process_start_time_seconds",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      }
   ]
}
```

### VM.CARDINALITY

#### Syntax

```
VKM.CARDINALITY FILTER selector... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VKM.SERIES** returns the number of unique time series that match a certain label set.

#### Options

- **MATCH**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

[Integer number](https://redis.io/docs/reference/protocol-spec#resp-integers) of unique time series.
The data section of the query result consists of a list of objects that contain the label name/value pairs which identify
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples
TODO


### VM.LABELS

#### Syntax

```
VM.LABELS FILTER selector... [START timestamp|rfc3339|+|*] [END timestamp|rfc3339|+|*]
```

**VKM.LABELS** returns a list of label names.

#### Options

- **selector**: Repeated series selector argument that selects the series to return. At least one match[] argument must be provided..
- **START**: Start timestamp, inclusive. Optional.
- **END**: End timestamp, inclusive. Optional.

#### Return

The data section of the JSON response is a list of string label names.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO

#### Examples

```
VKM.LABELS MATCH up process_start_time_seconds{job="prometheus"}
```
```json
{
   "status" : "success",
   "data" : [
      "__name__",
      "instance",
      "job"
   ]
}
```

### VKM.TOP-QUERIES

#### Syntax

```
VM.TOP-QUERIES [TOP_K number] [MAX_LIFETIME duration]
```

**VM.TOP-QUERIES** provides information on the following query types:

* the most frequently executed queries - `topByCount`
* queries with the biggest average execution duration - `topByAvgDuration`
* queries that took the most time for execution - `topBySumDuration`

The number of returned queries can be limited via `TOP_K` argument. Old queries can be filtered out by `MAX_LIFETIME`.
For example, 

```sh
$ redis-cli
127.0.0.1:6379> VKM.TOP-QUERIES TOP_K 5 MAX_LIFETIME 30s
```

would return up to 5 queries per list, which were executed during the last 30 seconds.
The last `lastQueriesCount` queries with durations at least `minQueryDuration` can be
set via `lastQueriesCount` and `minQueryDuration` module arguments at startup.


#### Options

- **TOP_K**: the number of records to return per metric. Default 20.
- **MAX_LIFETIME**: period from the current timestamp to use for filtering.

#### Return

See example below...

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label values for the job label:
```
127.0.0.1:6379> VKM.TOP-QUERIES TOP_K 5 MAX_LIFETIME 30s
```
```json
{
  "topK": "5",
  "maxLifetime": "30s",
  "lastQueriesCount": 500,
  "minQueryDuration": "1ms",
  "topByCount": [
    {
      "query": "(node_nf_conntrack_entries / node_nf_conntrack_entries_limit) > 0.75",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "(\n    max(slo:sli_error:ratio_rate5m{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (14.4 * 0.2)) without (sloth_window)\n    and\n    max(slo:sli_error:ratio_rate1h{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (14.4 * 0.2)) without (sloth_window)\n)\nor\n(\n    max(slo:sli_error:ratio_rate30m{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (6 * 0.2)) without (sloth_window)\n    and\n    max(slo:sli_error:ratio_rate6h{sloth_id=\"sandbox-vmcluster-requests-availability\", sloth_service=\"sandbox-vmcluster\", sloth_slo=\"requests-availability\"} > (6 * 0.2)) without (sloth_window)\n)\n",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "min(anomaly_score{preset=\"node-exporter\", for=\"host_network_transmit_errors\"}) without (model_alias, scheduler_alias)>=1.0",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "rate(node_network_transmit_errs_total[2m]) / rate(node_network_transmit_packets_total[2m]) > 0.01",
      "timeRangeSeconds": 0,
      "count": 20
    },
    {
      "query": "(process_max_fds {namespace=\"monitoring\"}- process_open_fds{namespace=\"monitoring\"}) \u003c 100",
      "timeRangeSeconds": 0,
      "count": 20
    }
  ],
  "topByAvgDuration": [
    {
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 \u003c 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) \u003c 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.286,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 15\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.234,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.230,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 20\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.202,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "avgDurationSeconds": 0.164,
      "count": 20
    }
  ],
  "topBySumDuration": [
    {
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 \u003c 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) \u003c 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 5.718,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 15\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.674,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.603,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_files{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 20\nand\n  predict_linear(node_filesystem_files_free{job=\"node-exporter\",fstype!=\"\"}[6h], 4*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 4.046,
      "count": 20
    },
    {
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 \u003c 40\nand\n  predict_linear(node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"}[6h], 24*60*60) \u003c 0\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "timeRangeSeconds": 0,
      "sumDurationSeconds": 3.284,
      "count": 20
    }
  ]
}
```

### VM.ACTIVE-QUERIES

#### Syntax

```
VM.ACTIVE-QUERIES
```

**VKM.ACTIVE-QUERIES** provides information on currently executing queries. It provides the following information per each query:

* The query itself, together with the time range and step args passed to /api/v1/query_range.
* The duration of the query execution.

```sh
$ redis-cli
127.0.0.1:6379> VKM.ACTIVE-QUERIES
```

#### Return

The data section of the JSON response is a list of string label values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label values for the job label:
```
127.0.0.1:6379> VKM.ACTIVE-QUERIES
```
```json
{
  "status": "ok",
  "data": [
    {
      "duration": "0.103s",
      "id": "17F248B7DFEEB024",
      "query": "(\n  node_filesystem_avail_bytes{job=\"node-exporter\",fstype!=\"\"} / node_filesystem_size_bytes{job=\"node-exporter\",fstype!=\"\"} * 100 < 3\nand\n  node_filesystem_readonly{job=\"node-exporter\",fstype!=\"\"} == 0\n)",
      "start": 1726080900000,
      "end": 1726080900000,
      "step": 300000
    },
    {
      "duration": "0.077s",
      "id": "17F248B7DFEEB025",
      "remote_addr": "10.71.10.4:44162",
      "query": "(node_filesystem_files_free{fstype!=\"msdosfs\"} / node_filesystem_files{fstype!=\"msdosfs\"} * 100 < 10 and predict_linear(node_filesystem_files_free{fstype!=\"msdosfs\"}[1h], 24 * 3600) < 0 and ON (instance, device, mountpoint) node_filesystem_readonly{fstype!=\"msdosfs\"} == 0) * on(instance) group_left (nodename) node_uname_info{nodename=~\".+\"}",
      "start": 1726080900000,
      "end": 1726080900000,
      "step": 300000
    }
  ]
}
```
## Acknowledgements
This underlying library this project uses originated as a heavily modded `rust` port of [VictoriaMetrics](https://victoriametrics.com).

## License
VKMetrics is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).