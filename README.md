# Coimbatore Ride Pulse

**Distributed Computing project: where and when a ride-hailing company in Coimbatore needs more drivers, or fewer, analysed with Hadoop MapReduce.**

* About 19.7 million simulated ride requests over 2 years (2024 and 2025), about 3 GB of CSV
* Covers 30 real Coimbatore areas: Gandhipuram, RS Puram, Peelamedu, Airport, Saravanampatti, Ukkadam and more
* Runs on a 4-node Hadoop 3.3.6 cluster in Docker (1 master and 3 workers), with HDFS replication 2
* Uses 6 chained MapReduce jobs (Hadoop Streaming, Python)
* Includes benchmarks: reducer count, combiner on/off, and data scaling
* Shows results on a dark dashboard with a Google Map (or a free OpenStreetMap), glowing and pulsing shortage zones, an hour slider and animated busy routes

> The ride data is **simulated**. No ride company publishes trip-level data for Indian cities. The generator builds realistic patterns (rush hours, IT-park shifts, airport flight times, rain, Pongal, Deepavali and New Year's Eve, and 2025 growth), and Hadoop has to discover them from the raw rows. Area coordinates are approximate area centres.

---

## 1. What you need

| | |
|---|---|
| OS | Windows 10/11 (tested setup: HP Victus, i5-12450H, 16 GB RAM) |
| Docker | Docker Desktop with the WSL 2 backend |
| Disk | ~15 GB free (image ~1.5 GB, raw data 3 GB, HDFS copies 6 GB, outputs) |
| Internet | Needed once, to build the image (downloads Hadoop from archive.apache.org) |

**Give Docker enough memory.** Create the file `C:\Users\<you>\.wslconfig` with:

```
[wsl2]
memory=10GB
processors=8
```

Then run `wsl --shutdown` in PowerShell and restart Docker Desktop.

## 2. Run it

**Easy way:** double-click **`RUN_ME.bat`**. It runs all 3 steps below and opens the dashboard.

**Manual way** (PowerShell, inside this folder):

```powershell
docker compose up -d --build                 # 1. start the cluster (first build: 5-10 min)
docker exec -it master bash /project/scripts/run_all.sh   # 2. data + jobs + benchmarks
start http://localhost:8000                  # 3. dashboard
```

To skip the benchmarks (about 20 min), add `-e SKIP_BENCH=1` after `exec`. For a quick test with less data, add `-e RIDES_PER_DAY=5000`.

| URL | What |
|---|---|
| http://localhost:8000 | Dashboard |
| http://localhost:9870 | HDFS NameNode UI: see the files, blocks and the 3 DataNodes |
| http://localhost:8088 | YARN UI: watch the jobs and map tasks run |
| http://localhost:19888 | MapReduce JobHistory (counters, task times) |

The dashboard already shows results when you open it, but **those numbers come from my local test run** (the same scripts joined with Unix pipes, with no cluster). The header says *"Local test run"*. After `run_all.sh` finishes on your laptop, it changes to *"Hadoop batch · last run …"*, and the Hadoop pages fill with your real timings.

Expected time on a 16 GB laptop is about 45-70 min in total: generating takes ~3 min, loading into HDFS ~3 min, the 6 jobs ~15-25 min and the benchmarks ~20-30 min.

## 3. Google Maps

Open `dashboard/config.js` and paste your key:

```js
googleMapsApiKey: "AIza...",
```

To get a key, go to Google Cloud Console, create a project, enable the **Maps JavaScript API**, and create an API key under **Credentials**. Google asks for billing details, but a student demo normally stays within the free monthly usage. Check Google's current pricing page to be sure.

Without a key, or if the key fails, the dashboard automatically uses a free dark OpenStreetMap / CARTO map. The zones, glow, pulse and routes look the same.

## 4. Architecture

```
 generate_rides.py ──► /staging/raw (24 monthly CSV, ~3 GB)
                              │ hdfs dfs -put
                              ▼
   HDFS  /rides/raw   24 files · ~24-40 blocks of 128 MB · replication 2 over worker1-3
                              │
   J1 Clean (map-only, counters) ──► /rides/clean
        ├──► J2 Zone×Hour stats (combiner) ──► J5 Shortage index ──► J6 Ranking (total-order sort)
        ├──► J3 Situations: year/day/month/rain/festival (combiner, 7 keys per ride)
        └──► J4 Top routes (custom KeyFieldBasedPartitioner by origin)
                              │ hdfs dfs -getmerge
                              ▼
   export_results.py ──► dashboard/data.js ──► index.html (Google Maps + Chart.js)
```

| Job | What it computes | Hadoop concept to explain in the viva |
|---|---|---|
| J1 Clean | Drops bad GPS, negative fares, clock-skew rows, truncated rows, bad status | **Map-only job**, custom **counters** (`DataQuality` group) |
| J2 Zone-hour | Requests, completed, no-driver, wait, long waits, idle drivers, fare and surge per area × weekday/weekend × hour × weather | **Combiner** (same sum script as the reducer) |
| J3 Situations | Yearly, per-area-per-year, hourly, drop-off, rain vs dry, festival, daily and monthly series | **Multiple key families** in one pass; the combiner shrinks the shuffle a lot |
| J4 Routes | Origin → destination counts per time band, top 6 per origin | **Custom partitioner**: all routes from one origin go to one reducer |
| J5 Shortage index | Scores 0-100 per area-hour and labels need drivers / balanced / surplus | **Job chaining** (input = J2 output) |
| J6 Ranking | All area-hours sorted by shortage | **Total-order sort** using the shuffle, 1 reducer |

**Shortage index** (computed in `jobs/j5_shortage_reducer.py`):

```
45 × min(no_driver_rate / 25%, 1)              riders who got no driver
+ 35 × min(max(avg_wait − 3 min, 0) / 12, 1)    how long riders waited
+ 20 × max(0, 1 − idle_drivers_nearby / 4)       supply near the rider
≥ 50 → Need drivers · ≥ 7 idle drivers, wait < 4.5 min, < 3% no-driver → Too many drivers
```

**Benchmarks** (`scripts/benchmark.sh`, results go to `output/benchmark_runs.jsonl`):

* J2 with 1, 2, 4 and 8 reducers
* J2 and J3 with the combiner on vs off (run time and shuffle bytes)
* J2 on 3, 6, 12 and 24 months of data

## 5. Folder map

```
RUN_ME.bat / OPEN_DASHBOARD.bat / STOP_CLUSTER.bat
docker-compose.yml          4 containers: master, worker1, worker2, worker3
docker/                     Dockerfile, start-node.sh, Hadoop XML configs
data/zones.csv              30 areas: coordinates, type, demand weight, driver supply ratio
data/generate_rides.py      data generator (numpy)
jobs/                       mappers, reducers, combiner (Python 3, standard library only)
scripts/run_all.sh          everything in order
scripts/generate_and_load.sh  generate + hdfs put + block report
scripts/run_pipeline.sh     the 6 jobs + getmerge + export
scripts/benchmark.sh        experiments
scripts/export_results.py   Hadoop output → dashboard/data.js
scripts/local_run.sh        no-Hadoop smoke test with Unix pipes
dashboard/                  index.html, config.js, data.js
output/                     run logs, counters, merged results
```

## 6. Columns in the raw data

`ride_id, request_time, pickup_time, dropoff_time, pickup_zone, drop_zone, pickup_lat, pickup_lng, drop_lat, drop_lng, distance_km, fare, surge, status, drivers_nearby, weather, festival`

* `status` is `completed`, `no_driver` or `cancelled`. Pickup and drop-off times are empty when the ride didn't happen.
* Wait time = `pickup_time − request_time`.
* `drivers_nearby` is the number of idle drivers within reach when the ride was requested (the supply signal).
* About 0.4% of rows are deliberately broken for J1 to catch.

## 7. If something goes wrong

* **`docker compose` fails at "curl … hadoop.tar.gz"**: archive.apache.org is slow or blocked. Use a mirror:
  `docker compose build --build-arg HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz`
* **Port 8000, 8088 or 9870 already in use**: change the left number in `docker-compose.yml`, for example `"8001:8000"`.
* **A job hangs at "map 0% reduce 0%"**: YARN has no memory left. Raise `.wslconfig` memory, or close other apps. Check http://localhost:8088 to see whether 3 nodes are listed.
* **`$'\r': command not found`**: Windows line endings. Run `docker exec master bash -c "sed -i 's/\r$//' /project/scripts/*.sh"`.
* **A job failed**: read `output/logs/<job name>.log`. Task logs are at http://localhost:19888.
* **Start completely fresh**: `docker compose down -v`, then run `RUN_ME.bat` again.
* **Test the logic without Hadoop** (Linux/WSL): `python3 data/generate_rides.py --out /tmp/raw --rides-per-day 3000 && bash scripts/local_run.sh /tmp/raw`
