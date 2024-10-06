```
VM.UNION fromTimestamp toTimestamp FILTER series_selector...
    [COUNT count]
    [WITHLABELS]
    [SELECTED_LABELS label...]
    [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP bt] EMPTY]
```

Return multiple series matched by timestamp. Similar to `MRANGE`, but inner joined by timestamp

Suppose we are tracking samples of the last 6 hours of latency of an ecommerce system across US regions in `aws`,
we could use a query such as the following:

```aiignore
VM.UNION -6hr * FILTER latency{service="cart", region~="us-*"} WITHLABELS
```
This would pull back data for `us-east-1`, `us-east-2`, `us-west-1` etc. 