# CloudLab Deployment Notes

A running log of important findings, gotchas, and non-obvious fixes encountered during MoStream CloudLab deployments. Check here before debugging to avoid re-discovering known issues.

---

## Flink 2.0 Web UI Metrics Show "loading..." (MetricQueryService on loopback)

**Symptom:** All operators show "loading..." for Bytes Received, Records Received, etc. in the Flink web UI. JM log repeatedly shows:
```
WARN org.apache.pekko.remote.ReliableDeliverySupervisor - Association with remote system
[pekko.tcp://flink-metrics@10.10.1.2:PORT] has failed ... Connection refused
```

**Root cause:** Flink 2.0's `MetricQueryService` hardcodes `127.0.0.1` as its bind address. It advertises the TM's external IP to the JM, but the JM's connection to that external IP is refused because the actual listener is on loopback. No Flink config (`taskmanager.host`, `taskmanager.bind-host`, etc.) overrides this — it's internal Flink 2.0 behavior.

**Why it varies across CloudLab allocations:** On some hardware, `hostname -i` returns the experiment network IP (`10.10.1.2`) which Flink can bind to successfully. On others, it returns the public IP (`128.110.96.30`), and Flink's fallback lands on loopback. Different physical machines → different behavior.

**Fix:** Pin the metrics port to a fixed value and use `socat` as a relay:

1. Add to TM's `~/flink/conf/config.yaml`:
   ```yaml
   metrics.internal.query-service.port: 9998
   ```

2. After TM starts, run on TM:
   ```bash
   nohup socat TCP-LISTEN:9998,bind=10.10.1.2,reuseaddr,fork TCP:127.0.0.1:9998 > /tmp/socat-metrics.log 2>&1 &
   ```

3. Verify with:
   ```bash
   ss -tlnp | grep 9998
   # Should show: socat on 10.10.1.2:9998 AND java on 127.0.0.1:9998
   ```

**This is automated in `setup_taskmanager.sh`.** If TM restarts, re-run setup or manually restart socat.

---

## Flink 2.0 Kafka Connector JAR

Use `flink-connector-kafka-4.0.1-2.0.jar` (NOT `3.1.0-1.17.jar`). The 1.17 JAR uses Flink's old `sink2` API removed in Flink 2.0, causing `NoClassDefFoundError: TwoPhaseCommittingSink` at job submission.

---

## TM JVM Metaspace OOM

Default Metaspace (256m) is too small for PyFlink + Beam + Kafka class loading.
Add to TM config:
```yaml
taskmanager.memory.jvm-metaspace.size: 512m
taskmanager.memory.process.size: 4000m
```
Without the `process.size` increase, you get `IllegalConfigurationException: Derived JVM Overhead size not in [192mb, 1024mb]`.

---

## conda activate fails on fresh SSH ("Run 'conda init' before 'conda activate'")

```bash
source ~/miniconda3/etc/profile.d/conda.sh && conda activate mostream
```
Not just `conda activate mostream`. The `source` step is required in non-interactive sessions.

---

## OpenSSL mismatch on git push (WSL2 with conda)

conda's `LD_LIBRARY_PATH` loads a newer OpenSSL than the system git expects:
```
OpenSSL version mismatch. Built against 30000020, you have 30600020
```
Fix: `env -u LD_LIBRARY_PATH git push ...`
Or add alias: `alias git='env -u LD_LIBRARY_PATH git'`

---

## TF threading config crashes PyFlink workers

`tf.config.threading.set_intra_op_parallelism_threads()` called after TF eager context is initialized causes:
```
RuntimeError: Intra op parallelism cannot be modified after initialization
```
Simply remove both threading lines — TF manages threads fine without them.

---

## Simulator usage

Simulator does NOT accept `--topic` or `--bootstrap` flags. Usage:
```bash
KAFKA_BOOTSTRAP=10.10.1.3:9092 python simulator.py --interval 1.0
```
Topic is hardcoded to `Simulation`; bootstrap server comes from the env var.

---

## TM crashes after ~18 hours: Direct buffer OOM → RegistrationTimeoutException

**Symptom:** TM runs fine for many hours then dies. Last lines in TM log:
```
OutOfMemoryError: Cannot reserve 4194304 bytes of direct buffer memory
  (allocated: 1339697345, limit: 1342177280)
...
RegistrationTimeoutException: Could not register at the ResourceManager within PT5M. Terminating now.
```

**Root cause:** Flink sets `-XX:MaxDirectMemorySize` to the sum of all configured direct memory:
`task.off-heap (512m) + framework.off-heap (256m) + network (512m) = 1280m`

Over 18+ hours of continuous PyFlink operation (TensorFlow JNI buffers, Beam gRPC channel buffers, Netty pooled arenas), this 1280 MB fills up completely. When full, any Netty buffer allocation fails → RM connection drops → after 5-min retry timeout → TM exits with `RegistrationTimeoutException`.

**The OOM causes the RM disconnect, not the other way around.** The `allocated` value stays frozen at ~1277 MB across all retry attempts — confirming the memory was already full before reconnection attempts.

**Fix:** Increase `task.off-heap.size` so TF/Beam buffers have room:
```yaml
taskmanager.memory.task.off-heap.size: 1024m   # was 512m
taskmanager.memory.process.size: 5000m          # was 4000m
```
This gives `MaxDirectMemorySize = 1024 + 256 + 512 = 1792m`, buying ~36+ hours per run.

**For currently deployed TM (apply without re-running setup):**
```bash
sed -i 's/taskmanager.memory.task.off-heap.size: 512m/taskmanager.memory.task.off-heap.size: 1024m/' ~/flink/conf/config.yaml
sed -i 's/taskmanager.memory.process.size: 4000m/taskmanager.memory.process.size: 5000m/' ~/flink/conf/config.yaml
~/flink/bin/taskmanager.sh start
TM_IP=$(hostname -I | tr ' ' '\n' | grep '^10\.10\.1\.' | head -1)
nohup socat TCP-LISTEN:9998,bind=${TM_IP},reuseaddr,fork TCP:127.0.0.1:9998 > /tmp/socat-metrics.log 2>&1 &
```

**Fixed in `setup_taskmanager.sh`** (task.off-heap is now 1024m).

---

## Periodic task restarts every ~10 minutes (Beam Python environment recycling)

**Symptom:** All Flink tasks restart every ~10 minutes. TM log shows:
```
FnApiControlClient closed, clearing outstanding requests
Task ... switched to CANCELING
```
Web UI shows attempt number incrementing (#2, #3, #4...) and record counters resetting.
TM is alive and healthy — only tasks restart.

**Root cause:** Beam automatically recycles Python worker environments every ~10 minutes (`environmentCacheExpiryMillis` default ≈ 600s). When it expires an environment while bundles are in flight, Flink sees a task failure → region failover → all tasks in the region cancel and restart. Recovery takes ~5 seconds.

**This is NOT OOM.** No `OutOfMemoryError` in the log when this happens.

**Fix:** Increase `python.fn-execution.bundle.size` in MDWorkflow.py to reduce recycle frequency:
```python
config.set_string("python.fn-execution.bundle.size", "100")  # was 1
```
With `bundle.size=1`, each record is its own bundle and recycle is triggered more readily. With 100, Beam accumulates 100 records per bundle which spaces out the recycling.

**Acceptable behavior:** If restarts persist, 5-second downtime every 10 min (~0.8%) is tolerable.

---

## Kafka consumer offset monitoring

Check pipeline health via Kafka offsets (more reliable than Flink web UI for PyFlink):
```bash
~/kafka/bin/kafka-get-offsets.sh --bootstrap-server 10.10.1.3:9092 --topic-partitions Simulation:0,Recommend:0
~/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 10.10.1.3:9092 --describe --group my-group
```
Recommend offset growing = pipeline is producing recommendations end-to-end.
