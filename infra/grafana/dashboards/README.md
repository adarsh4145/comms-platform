# Dashboards

Provisioned into Grafana's `comms-platform` folder by `../dashboards-provider.yaml`, which the
`lgtm` service bind-mounts. Everything here is committed, so a fresh clone gets the same Grafana.

| File | Source | Local changes |
|---|---|---|
| `platform-health.json` | written here | — |
| `jvm-micrometer.json` | grafana.com **4701** rev 10 | `application` dropdown scoped to `up{job="comms-apps"}` |
| `springboot-apm.json` | grafana.com **12900** rev 3 | same |
| `kafka-exporter.json` | grafana.com **7589** rev 5 | none |
| `postgres.json` | grafana.com **9628** rev 8 | none - the labels it wants are supplied by `infra/prometheus.yaml` |
| `mongodb.json` | grafana.com **2583** rev 2 | none |
| `redis.json` | grafana.com **763** rev 6 | none - `namespace` label supplied by `infra/prometheus.yaml` |
| `cadvisor.json` | grafana.com **19908** rev 1 | added a `container` dropdown; the eight panel queries now filter on it instead of `name=~".+"` |

Every downloaded file had its `__inputs`/`__requires` stripped and `${DS_*}` replaced with the
`prometheus` datasource uid - a provisioned dashboard cannot stop and ask which datasource to use.

Two of the community dashboards are written for Helm/Kubernetes deployments and key their
dropdowns off labels that do not exist here (`namespace`, `release`, `kubernetes_namespace`).
Rather than rewrite them - and then own that diff against every future revision - those labels are
attached to the exporter targets in `infra/prometheus.yaml`. If a dropdown ever goes empty after
re-downloading a newer revision, check there first.

`dashboards-provider.yaml` sets `allowUiUpdates: false`, so Grafana will not let you save edits to
these dashboards in the browser - provisioning re-applies the files on every start. That is
deliberate: the lgtm container keeps Grafana's database on a persisted volume, so a UI edit would
otherwise outlive the container and shadow the committed file. Edit the JSON here instead.

## Things that look broken and are not

**Kafka Exporter Overview shows lag of `-1`.** That is not negative lag. Kafka returns `-1` as the
committed offset for a partition a consumer group has never committed on, and kafka-exporter passes
the sentinel straight through. With four partitions per topic and light traffic, most partitions
have simply never been read from. It becomes a real number once each partition sees a message. The
tell is that the value is always exactly `-1`, never drifting.

**JVM (Micrometer) looks blank for the first few seconds.** It fires roughly 30 queries on load and
paints nothing until they return; a screenshot taken too early shows every panel empty. Wait for it.

**JVM (Micrometer): "Utilisation" and "File Descriptors" are empty for the app services.**
Utilisation queries `tomcat_threads_*` and `jetty_threads_*`, and the five app services run on
Netty - that panel is only meaningful for `configServer` and `eurekaServer`. File Descriptors
queries `process_files_open_files`, which Micrometer only binds on Unix, so it will never have data
on Windows.

**Loki: the trace link is under "Links", not next to the value.** Expanding a log row shows
`trace_id` twice - once in the fields list, where it only offers copy and filter, and once under
**Links** as a `Trace: <id>` chip that jumps to Tempo. The second one is the link.
