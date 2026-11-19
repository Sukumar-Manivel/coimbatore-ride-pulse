# COIMBATORE RIDE PULSE: URBAN RIDE-HAILING DEMAND AND SUPPLY ANALYTICS USING DISTRIBUTED HADOOP MAPREDUCE

**A PROJECT REPORT**

Submitted by  
**TEAM 17**  
* 715524104165  
* 715524104166  
* 715524104182  

in partial fulfilment of the requirements for the subject  
**CS3551 – DISTRIBUTED COMPUTING**

**DEPARTMENT OF COMPUTER SCIENCE AND ENGINEERING**  
**PSG INSTITUTE OF TECHNOLOGY AND APPLIED RESEARCH**  
*(An Autonomous Institution, Approved by AICTE and Affiliated to Anna University)*  
**NEELAMBUR, COIMBATORE – 641 062**  
**NOVEMBER 2026**

---

## DECLARATION

We affirm that the Project Report titled **COIMBATORE RIDE PULSE: URBAN RIDE-HAILING DEMAND AND SUPPLY ANALYTICS USING DISTRIBUTED HADOOP MAPREDUCE** being submitted in partial fulfilment of the requirements for the subject **“CS3551 – Distributed Computing”** is the original work carried out by us. It has not formed part of any other project report or dissertation on the basis of which a degree or award was conferred on an earlier occasion on this or any other candidate.

**Date:**  
**(Signature of the Candidates)**  
**TEAM 17**  
* 715524104165  
* 715524104166  
* 715524104182  

I certify that the declaration made by the above candidates is true to the best of my knowledge.

**Date:**  
**Name and Signature of the Course Faculty**

---

## ABSTRACT

Urban ride-hailing services require continuous alignment between driver supply and rider demand. However, in expanding urban hubs like Coimbatore, mobility patterns shift dynamically across zones and operating hours: transit terminals peak at dawn, major IT and college corridors surge during morning and evening shift changeovers, commercial centres peak during midday, and the airport experiences high unmet demand during late-night arrival windows. When drivers fail to follow these spatial and temporal rhythms, riders experience unacceptable waiting delays or complete service unavailability, while idle drivers congregate unproductively in low-demand areas. Analysing multi-million ride request archives sequentially on single machines is computationally prohibitive and fails to provide actionable, multi-dimensional dispatch insights.

This project presents **Coimbatore Ride Pulse**, a production-grade distributed big-data analytics system designed to quantify and predict ride-hailing imbalances across 30 distinct zones of Coimbatore. The project operates on a comprehensive two-year simulated dataset of 19,709,930 ride requests (~3.18 GB) spanning 2024 and 2025 across 17 attributes, incorporating realistic rush hours, shift schedules, monsoon precipitation events, cultural festivals, and deliberate data anomalies (~0.4% dirty rows). The storage architecture utilizes the Hadoop Distributed File System (HDFS) configured across a 4-node Dockerized cluster (1 Master NameNode/ResourceManager and 3 Worker DataNodes/NodeManagers) with a block size of 128 MB and replication factor 2. A modular six-stage MapReduce pipeline, implemented via Hadoop Streaming in Python, performs map-only data cleaning with custom counters (Job 1), zone-hour multi-metric aggregation using associative combiners (Job 2), high-dimensional situational and temporal cubing (Job 3), origin-destination time-band routing via a custom KeyFieldBasedPartitioner (Job 4), composite driver shortage scoring (Job 5), and global hotspot total-order sorting using Hadoop's native shuffle mechanism (Job 6).

Empirical evaluations and benchmarks demonstrate near-linear data scaling across 3, 6, 12, and 24 months, optimal task speedup across 1 to 8 reducers, and an 85–92% reduction in network shuffle I/O achieved by the combiner. The pipeline outcomes are delivered through an interactive dark-themed dashboard integrating Leaflet, Google Maps, and Chart.js, visualizing animated shortage pulses, wait-time heatmaps, and peak corridor flows. Findings reveal that while 26 of 30 areas experience idle driver surpluses at midday, unfulfilled ride rates jump from 6.4% in dry weather to 17.9% during heavy rain (+61% wait time), peaking at 41.2% at the Airport during weekend nights and exceeding 28% on major festival evenings. The system validates the power of containerized Hadoop clusters for high-throughput urban mobility engineering.

---

## TABLE OF CONTENTS

1. **INTRODUCTION** ................................................................ 1  
   1.1 Overview of Urban Ride-Hailing Analytics ....................................... 1  
   1.2 Background ..................................................................... 1  
   1.3 Problem Statement .............................................................. 1  
   1.4 Objectives ..................................................................... 2  
   1.5 Scope of the Project ........................................................... 2  
2. **LITERATURE REVIEW AND EXISTING SYSTEM** ...................................... 3  
   2.1 Introduction ................................................................... 3  
   2.2 Existing System ................................................................ 3  
   2.3 Limitations of Existing System ................................................. 3  
   2.4 Proposed System ................................................................ 3  
   2.5 Advantages of Proposed System .................................................. 3  
3. **SYSTEM REQUIREMENTS AND TECHNOLOGIES** ....................................... 4  
   3.1 Hardware Requirements .......................................................... 4  
   3.2 Software Requirements .......................................................... 4  
   3.3 Python ......................................................................... 4  
   3.4 Web Dashboard Architecture ..................................................... 4  
   3.5 Apache Hadoop .................................................................. 5  
   3.6 HDFS ........................................................................... 5  
   3.7 YARN ........................................................................... 5  
   3.8 Hadoop Streaming and MapReduce ................................................. 5  
   3.9 Docker and Docker Compose ...................................................... 5  
   3.10 HTML, CSS and JavaScript ...................................................... 5  
   3.11 Google Maps, Leaflet and Chart.js ............................................. 5  
4. **SYSTEM DESIGN AND ARCHITECTURE** ............................................. 6  
   4.1 Overall System Architecture .................................................... 6  
   4.2 System Workflow ................................................................ 7  
   4.3 Module Architecture ............................................................ 8  
   4.4 Data Flow Diagram .............................................................. 8  
   4.5 HDFS Directory Structure ....................................................... 9  
   4.6 Hadoop Cluster Node Architecture ............................................... 9  
   4.7 YARN Processing Architecture ................................................... 10  
5. **IMPLEMENTATION** ............................................................. 11  
   5.1 Project Structure .............................................................. 11  
   5.2 Dataset Description ............................................................ 11  
   5.3 Data Generation and Ingestion .................................................. 11  
   5.4 Data Cleaning with Hadoop Counters (Job 1) ..................................... 11  
   5.5 HDFS Data Storage and Block Replication ........................................ 12  
   5.6 MapReduce Job 2: Zone-Hour Aggregation and Combiner ............................ 12  
   5.7 MapReduce Job 3: Multi-Family Situational Analytics Cube ....................... 12  
   5.8 MapReduce Job 4: Top Routes with Custom Partitioner ............................ 12  
   5.9 MapReduce Job 5: Driver Shortage Index Algorithm ............................... 13  
   5.10 MapReduce Job 6: Total-Order Sort Hotspot Ranking ............................. 13  
   5.11 Interactive Dashboard Implementation .......................................... 13  
6. **RESULTS AND DISCUSSION** ..................................................... 14  
   6.1 Application Output ............................................................. 14  
   6.2 Demand and Driver Supply Analytics ............................................. 14  
   6.3 Wait-Time Heatmap Analytics .................................................... 15  
   6.4 Weather and Festival Impact Analytics .......................................... 15  
   6.5 Top Shortage Hotspots and Route Ranking ........................................ 16  
   6.6 HDFS Storage and Replication Results ........................................... 16  
   6.7 YARN Execution and Counter Metrics ............................................. 16  
   6.8 Hadoop Cluster Benchmarks ...................................................... 16  
   6.9 Testing and Validation ......................................................... 16  
7. **CONCLUSION AND FUTURE ENHANCEMENTS** ......................................... 17  
   7.1 Conclusion ..................................................................... 17  
   7.2 Advantages ..................................................................... 17  
   7.3 Limitations .................................................................... 17  
   7.4 Future Enhancements ............................................................ 17  
**REFERENCES** ..................................................................... 18  
**APPENDIX** ....................................................................... 19  

---

## 1. INTRODUCTION

### 1.1 OVERVIEW OF URBAN RIDE-HAILING ANALYTICS
Urban ride-hailing platforms such as Uber, Ola, and Rapido operate in hyper-dynamic transport environments where matching rider demand with driver supply is a critical operational objective. At city scale, millions of ride requests are logged annually, capturing timestamps, geographic coordinates, rider wait durations, fare structures, surge pricing multipliers, vehicle availability, and terminal completion statuses. Analyzing these massive streaming logs provides actionable intelligence that allows mobility operators to reposition idle drivers, optimize dynamic pricing policies, mitigate prolonged passenger wait times, and curb unfulfilled ride requests.

In Coimbatore, the industrial and educational capital of western Tamil Nadu, geographic demand is characterized by sharp sectoral variations. Passenger flow alternates between central transit hubs (Gandhipuram, Ukkadam, Coimbatore Junction), major IT and educational corridors (Saravanampatti, Peelamedu, Hope College, Kalapatti), commercial retail districts (RS Puram, Town Hall, Race Course), and vital transport gateways like Coimbatore International Airport. Transforming high-volume raw trip logs into hourly spatial shortage indicators enables operations teams to eliminate systemic supply gaps across the city.

### 1.2 BACKGROUND
Traditional urban transit analysis has historically relied on static sampling surveys, localized sensor loops, or standalone relational databases. However, modern ride-hailing logs accumulate continuously, yielding multi-gigabyte archives characterized by extreme volume, heterogeneous formats, GPS inaccuracies, clock skews, and missing attribute values. Processing multi-year urban ride histories sequentially on desktop-class workstations results in prohibitive memory bottlenecks, extended runtimes, and vulnerability to job crashes.

Apache Hadoop provides an enterprise distributed infrastructure comprising the Hadoop Distributed File System (HDFS) for resilient, fault-tolerant block-level storage and Yet Another Resource Negotiator (YARN) for distributed job scheduling and resource orchestration. Hadoop MapReduce enables high-throughput batch parallelization across compute nodes. By virtualizing a multi-node Hadoop cluster within Docker, complex distributed data pipelines can be deployed reproducibly on accessible computing hardware, providing a scalable testbed for urban transit optimization.

### 1.3 PROBLEM STATEMENT
Ride demand across Coimbatore exhibits severe temporal and geographic volatility. Peak passenger requests concentrate heavily during morning and evening rush hours along IT and college corridors, during dawn and dusk at central bus and railway junctions, and late at night around airport terminals. Conversely, driver availability is skewed toward central commercial zones where drivers remain idle during midday hours. This spatial friction produces acute driver shortages in outlying zones, extended wait times exceeding 15 minutes, elevated passenger cancellations, and lost platform revenue.

Furthermore, raw mobility data contains corrupt GPS records, negative fares, mismatched timestamps, and truncated fields. Existing analytical workflows mix data cleansing, aggregation, and scoring into monolithic scripts on a single computer, lacking automated fault recovery, block-level parallelism, or scalable multidimensional grouping. A distributed computing framework is required that can ingest multi-gigabyte trip archives into HDFS, clean and validate rows in parallel, compute multi-dimensional aggregation cubes, score driver shortage indices, and rank critical supply bottlenecks for real-time visualization.

### 1.4 OBJECTIVES
* To store 2 years of ride-request records (~19.7 million trips, 3.18 GB) in HDFS with replication across 3 DataNode workers.
* To perform distributed data cleaning and anomaly detection using a map-only MapReduce job with custom Hadoop counters.
* To compute high-dimensional demand and supply aggregations across area, day type, hour, and weather conditions using associative combiners.
* To quantify multi-family situational analytics, comparing seasonal demand, monsoon rain impacts, and festival surges.
* To isolate high-traffic origin-destination corridors using a custom KeyFieldBasedPartitioner to rank top routes per zone.
* To compute a 0–100 composite driver shortage index and rank the most severe citywide shortage hotspots using total-order sorting.
* To benchmark Hadoop cluster scalability across variable reducer allocations (1–8), combiner execution (ON/OFF), and data volumes.
* To render insights via an interactive, containerized web dashboard featuring interactive maps, heatmaps, and performance charts.

### 1.5 SCOPE OF THE PROJECT
The system analyzes trip-level ride requests across 30 recognized geographic zones in Coimbatore over a 24-month horizon (January 2024 to December 2025). The Hadoop cluster is implemented in Docker comprising 1 master node and 3 worker nodes running Hadoop 3.3.6 under Ubuntu on WSL2. Analytics encompass demand volumes, completed rides, unserved requests, waiting times, idle vehicle availability, route frequencies, rain weather impacts, and 7 major festivals.

The project focuses on batch analytics and distributed parallel processing. Real-time sub-second GPS telemetry ingestion, live streaming vehicle navigation, dynamic surge price optimization, and predictive machine learning models are outside the present implementation scope.

---

## 2. LITERATURE REVIEW AND EXISTING SYSTEM

### 2.1 INTRODUCTION
This section examines contemporary methodologies utilized for transportation data processing, delineates the structural bottlenecks inherent in traditional single-machine analysis pipelines, and demonstrates how the proposed distributed Hadoop architecture overcomes these limitations.

### 2.2 EXISTING SYSTEM
In conventional enterprise and municipal setups, ride-request logs are periodically exported from transactional databases into relational data warehouses or flat CSV files. Analysts typically process these datasets using standalone Python scripts (utilizing Pandas or NumPy) or desktop spreadsheet software on individual workstations. Data validation, metric aggregation, and report generation are executed sequentially, requiring all records to reside in local system memory.

### 2.3 LIMITATIONS OF EXISTING SYSTEM
* **Memory Exhaustion:** Datasets spanning millions of rows rapidly exceed desktop RAM limits, inducing swapping and runtime crashes.
* **Sequential Processing Bottlenecks:** CPU execution is tied to single-machine threads, causing aggregation runtimes to scale poorly.
* **Late Anomaly Discovery:** Data quality checks are embedded within downstream scripts, allowing corrupted records to distort metrics.
* **Monolithic Code Coupling:** Data parsing, validation, aggregation, and ranking are tightly coupled; any failure requires restarting from scratch.
* **No Managed Fault Tolerance:** If an analysis script fails midway through execution, all intermediate progress is lost without automated task retries.
* **Static Reporting:** Outputs are static tables or exported figures that fail to provide interactive spatial and temporal exploration.

### 2.4 PROPOSED SYSTEM
The proposed Coimbatore Ride Pulse architecture implements an enterprise-grade distributed computing pipeline. Raw ride records (19.7 million rows across 24 monthly CSVs) are ingested into HDFS across a 4-node cluster, where each 128 MB block is replicated across two worker nodes. A 6-stage MapReduce pipeline orchestrated by YARN executes distributed data cleansing, multidimensional aggregation, route partitioning, shortage index computation, and global ranking. Combiners minimize network shuffle overhead, while a custom partitioner groups origin routes efficiently. The final structured results are merged into an interactive dark-themed web dashboard.

### 2.5 ADVANTAGES OF PROPOSED SYSTEM
* **Distributed Scalability:** Workloads are divided across independent worker nodes, scaling seamlessly with multi-gigabyte datasets.
* **Automated Fault Tolerance:** HDFS block replication (factor 2) and YARN task re-execution prevent data loss and job failures.
* **High-Throughput In-Network Reduction:** Combiners aggregate key-value pairs at the mapper stage, slashing network shuffle traffic by up to 92%.
* **Specialized Pipeline Modularity:** Independent chained jobs isolate data cleansing, zone aggregation, route discovery, and ranking.
* **Real-time Interactive Exploration:** Interactive web dashboard with time-sliders, heatmaps, and glowing geographic shortage zones.

---

## 3. SYSTEM REQUIREMENTS AND TECHNOLOGIES

### 3.1 HARDWARE REQUIREMENTS
| Component | Recommended Specification |
|---|---|
| **Processor** | 64-bit Intel Core i5/i7 (8 cores or more) |
| **Memory (RAM)** | 16 GB DDR4/DDR5 (minimum 10 GB allocated to WSL 2) |
| **Disk Storage** | 25 GB free solid-state drive (SSD) storage |
| **Host Operating System** | Windows 11 / Windows 10 (64-bit) with WSL 2 enabled |
| **Display** | 1920 x 1080 resolution display for web dashboard |

### 3.2 SOFTWARE REQUIREMENTS
| Software Tool | Functional Role in Project |
|---|---|
| **Ubuntu 22.04 on WSL 2** | Host runtime environment for Docker containerization |
| **Docker Desktop & Compose** | Container virtualization for 1 Master + 3 Worker nodes |
| **Apache Hadoop 3.3.6** | Distributed file storage (HDFS), scheduling (YARN), MapReduce |
| **Java 8 (Eclipse Temurin)** | Underlying Java virtual machine runtime for Hadoop daemons |
| **Python 3.11** | Programming language for Mappers, Reducers, Combiners, Scripts |
| **NumPy** | High-performance vector operations for synthetic ride generation |
| **HTML5, CSS3, JavaScript** | Front-end structure, dark UI styling, and dashboard interactions |
| **Leaflet / Google Maps API** | Geographic map rendering with pulse rings and route polylines |
| **Chart.js** | Interactive canvas visualizations for hourly curves and benchmarks |

### 3.3 PYTHON
Python 3 serves as the primary programming language for all MapReduce logic, synthetic data generation, and result exporting. Using Python's standard library guarantees zero external dependencies inside worker containers, while standard stream processing (`sys.stdin` and `sys.stdout`) integrates natively with Hadoop Streaming.

### 3.4 WEB DASHBOARD ARCHITECTURE
The front-end architecture is served by a lightweight Python HTTP server on port 8000. It reads pre-aggregated analytical structures from `data.js` without requiring database queries, achieving near-instantaneous client-side filtering across zones, hours, and weather conditions.

### 3.5 APACHE HADOOP
Apache Hadoop 3.3.6 provides the core framework for distributed batch computation. It abstracts physical storage into a unified virtual filesystem and schedules execution containers across multiple worker instances, ensuring linear computational throughput.

### 3.6 HDFS
The Hadoop Distributed File System partitions multi-gigabyte CSV files into 128 MB blocks distributed across DataNodes. Block-level replication ensures that the loss of any single worker node does not interrupt pipeline execution or cause data corruption.

### 3.7 YARN
Yet Another Resource Negotiator decouples resource allocation from the application layer. The ResourceManager on `master` receives MapReduce job requests and allocates task containers across NodeManagers on `worker1`, `worker2`, and `worker3`, tracking applications through SUBMITTED, ACCEPTED, RUNNING, and FINISHED states.

### 3.8 HADOOP STREAMING AND MAPREDUCE
Hadoop Streaming allows any executable or script to serve as mappers and reducers via standard UNIX streams. Map tasks read line-delimited records from stdin and emit tab-separated key-value pairs to stdout, which the Hadoop framework sorts and shuffles before reducer consumption.

### 3.9 DOCKER AND DOCKER COMPOSE
Docker packages Hadoop daemons and configuration files into reproducible image layers. Docker Compose orchestrates a 4-container topology (`master`, `worker1`, `worker2`, `worker3`) connected via a dedicated bridge network (`ride-pulse-net`), mapping host ports 9870 (NameNode), 8088 (YARN), 19888 (JobHistory), and 8000 (Dashboard).

### 3.10 HTML, CSS AND JAVASCRIPT
The web front end employs modern semantic HTML5 and vanilla JavaScript ES6. Dark-themed visual styling is driven by custom CSS3 variables, animations, and gradients, maintaining 60 FPS performance during slider interactions.

### 3.11 GOOGLE MAPS, LEAFLET AND CHART.JS
Spatial visualization uses Leaflet as the primary zero-cost engine, with optional Google Maps API integration. Pulsating CSS keyframe rings represent shortage severity, while Chart.js renders demand distributions, hourly wait curves, and cluster benchmark comparisons.

---

## 4. SYSTEM DESIGN AND ARCHITECTURE

### 4.1 OVERALL SYSTEM ARCHITECTURE
The system architecture is organized into four distinct operational tiers: Data Source Tier, Distributed Storage Tier, Parallel Processing Tier, and Presentation Tier.

* **Master Node:** Runs NameNode (HDFS metadata), ResourceManager (YARN task scheduler), JobHistory Server, and Dashboard HTTP server.
* **Worker Nodes (worker1, worker2, worker3):** Each runs DataNode (block storage) and NodeManager (task execution) daemons.
* **Storage Allocation:** 128 MB blocks distributed with replication factor 2.
* **Compute Placement:** YARN dispatches map tasks directly to the node hosting the target data block (data locality), eliminating unnecessary inter-node network bandwidth.

### 4.2 SYSTEM WORKFLOW
The end-to-end data pipeline executes in 8 structured stages:
1. **Stage 1 - Synthetic Data Generation:** `generate_rides.py` simulates 19.7M requests across 24 monthly CSVs with rush hours, weather, and 0.4% dirty rows.
2. **Stage 2 - HDFS Loading:** `generate_and_load.sh` ingests CSVs into HDFS `/rides/raw` with 128 MB blocks and replication 2.
3. **Stage 3 - Job 1 (Data Cleanse):** Map-only job validates rows, emits custom counters, and writes clean data to `/rides/clean`.
4. **Stage 4 - Jobs 2, 3, 4 (Parallel Analytics):** Job 2 computes zone-hour metrics; Job 3 extracts situational series; Job 4 extracts route frequencies.
5. **Stage 5 - Job 5 (Shortage Scoring):** Chained job computes 0–100 shortage indices and categorical status (`need_drivers`, `balanced`, `surplus`).
6. **Stage 6 - Job 6 (Total-Order Sort):** Inverts shortage indices into sort keys (`1000 - index`), utilizing Hadoop shuffle to rank all 1,440 area-hours.
7. **Stage 7 - Result Extraction:** `hdfs dfs -getmerge` pulls TSV outputs to local disk; `export_results.py` builds `dashboard/data.js`.
8. **Stage 8 - Visual Dashboard:** Serves web UI on port 8000 with interactive maps, heatmaps, and benchmark plots.

### 4.3 MODULE ARCHITECTURE
| Module Component | Functional Description |
|---|---|
| `data/generate_rides.py` | NumPy data generator modeling rush hours, shifts, weather, festivals |
| `data/zones.csv` | Geographic coordinates, classification, demand weights of 30 zones |
| `jobs/rideparse.py` | Shared field validation, boundary checking, and counter reporting |
| `jobs/j1_clean_mapper.py` | Map-only validation emitting `DataQuality` Hadoop counters |
| `jobs/j2_zone_hour_mapper.py` | Zone-daytype-hour-weather mapper emitting 9-metric vectors |
| `jobs/sum_reducer.py` | Associative element-wise combiner and reducer for numerical metrics |
| `jobs/j3_situation_mapper.py` | Multi-key situational cube mapper (yearly, rain, festival, time-series) |
| `jobs/j4_routes_mapper.py / reducer.py` | Origin-destination time-band mapper with custom partitioner |
| `jobs/j5_shortage_mapper.py / reducer.py` | Chained shortage index scoring algorithm and classification |
| `jobs/j6_rank_mapper.py` | Shuffle-sort key inverter for global total-order ranking |
| `scripts/run_all.sh & lib.sh` | Cluster orchestration, safe-mode wait, and job execution wrapper |
| `scripts/benchmark.sh` | Automated scaling benchmarks (reducers, combiners, data volumes) |
| `dashboard/index.html & data.js` | Full-featured client dashboard with Leaflet and Chart.js |

### 4.4 DATA FLOW DIAGRAM
Raw data flows through a structured progression from ingestion to browser rendering. In Stage 1, raw CSV files (3.18 GB) are split across HDFS blocks. The Job 1 mapper streams lines, dropping invalid records and writing valid CSV rows to `/rides/clean`. Jobs 2, 3, and 4 process `/rides/clean` in parallel. Job 2 output is piped to Job 5, which computes the shortage index vector. Job 5 output is read by Job 6, which ranks the hotspots. Finally, `export_results.py` serializes all TSVs into JSON structures within `data.js`.

### 4.5 HDFS DIRECTORY STRUCTURE
* `/rides/raw/` : 24 monthly raw CSV files (`rides_2024_01.csv` to `rides_2025_12.csv`), ~3.18 GB.
* `/rides/clean/` : Cleansed CSV records from Job 1, ~3.16 GB, free of GPS, fare, or status errors.
* `/rides/out/j2_zone_hour/` : Aggregated zone-hour-weather metric vectors.
* `/rides/out/j3_situations/` : Multi-key situational cubes (yearly, festival, rain, daily, monthly).
* `/rides/out/j4_routes/` : Top 6 busiest travel routes per origin zone per time band.
* `/rides/out/j5_shortage/` : Shortage indices and status labels for 4,320 zone-hour combinations.
* `/rides/out/j6_ranking/` : Totally ordered list of all 1,440 unique zone-hour configurations.
* `/bench/` : Temporary HDFS directories for reducer and scaling benchmark runs.

### 4.6 HADOOP CLUSTER ARCHITECTURE
The cluster comprises 4 Linux containers running in Docker Compose. The `master` container acts as NameNode, YARN ResourceManager, JobHistory server, and web dashboard host. Containers `worker1`, `worker2`, and `worker3` operate as DataNodes and NodeManagers, each provisioned with 2 GB YARN container memory and 2 virtual cores. HDFS replication factor 2 guarantees that any single DataNode can fail without data unavailability.

### 4.7 YARN PROCESSING ARCHITECTURE
When `run_job` invokes Hadoop Streaming, the client connects to YARN ResourceManager on port 8032. The application transitions through four operational states: SUBMITTED, ACCEPTED (ResourceManager allocates container resources), RUNNING (NodeManagers execute mappers and reducers), and FINISHED (final status SUCCEEDED). Custom counters and task performance logs are archived to the JobHistory server on port 19888.

---

## 5. IMPLEMENTATION

### 5.1 PROJECT STRUCTURE
The project maintains a clean separation of concerns across directories: `data/` contains zone coordinates and data generator scripts; `jobs/` contains pure Python mappers, reducers, and validators; `scripts/` houses shell wrappers and benchmark harnesses; `docker/` contains Dockerfiles and Hadoop XML configuration files; `output/` stores run logs and results; and `dashboard/` contains the web visualization.

### 5.2 DATASET DESCRIPTION
The simulated dataset encompasses 19,709,930 trip records spanning 731 days (523 weekdays and 208 weekend days) across 30 Coimbatore areas. Each record comprises 17 comma-separated attributes:

| Field Name | Data Type | Description & Constraints |
|---|---|---|
| `ride_id` | String | Unique ride identifier (e.g., R2507931179) |
| `request_time` | Timestamp | ISO-8601 request timestamp (YYYY-MM-DD HH:MM:SS) |
| `pickup_time` | Timestamp | Pickup timestamp (empty for cancelled/no-driver) |
| `dropoff_time` | Timestamp | Drop-off timestamp (empty for unfulfilled rides) |
| `pickup_zone` | String | Origin zone name (one of 30 Coimbatore zones) |
| `drop_zone` | String | Destination zone name (one of 30 Coimbatore zones) |
| `pickup_lat/lng` | Float | Pickup GPS coordinates (10.85–11.20 N, 76.80–77.20 E) |
| `drop_lat/lng` | Float | Dropoff GPS coordinates (10.85–11.20 N, 76.80–77.20 E) |
| `distance_km` | Float | Trip travel distance in kilometers |
| `fare` | Float | Final ride fare in INR (must be non-negative) |
| `surge` | Float | Surge multiplier applied at request time (1.0x to 2.5x) |
| `status` | String | Ride outcome: completed, no_driver, cancelled |
| `drivers_nearby` | Integer | Idle drivers in zone at request time (supply signal) |
| `weather` | String | Weather state: clear or rain |
| `festival` | String | Festival label (pongal, deepavali, new_year, none) |

### 5.3 DATA GENERATION AND INGESTION
`generate_rides.py` models hourly demand using zone-type profiles (transit, commercial, IT/college, residential, industrial, airport). Rush hours, shift transitions (e.g., 6 PM IT peak), monsoon rainfall, and weekend leisure travel are parameterized. A supply curve models driver shifts, naturally inducing shortages when demand exceeds active driver supply. Data is saved as 24 monthly CSVs and loaded into HDFS via `hdfs dfs -put`.

### 5.4 DATA CLEANING WITH HADOOP COUNTERS (JOB 1)
Job 1 runs as a map-only job (`-reducer NONE`). `j1_clean_mapper.py` imports `rideparse.py` to validate coordinate boundaries, positive fares, valid statuses, non-negative wait times, and field counts. Bad rows are dropped, while problem categories are tracked via Hadoop Streaming counters written to stderr: `reporter:counter:DataQuality,<Reason>,<Count>`. Out of 19,709,930 rows, exactly 78,289 invalid rows (0.40%) were dropped, writing 19,631,641 clean rows to `/rides/clean`.

### 5.5 HDFS DATA STORAGE AND BLOCK REPLICATION
`generate_and_load.sh` partitions raw data across 128 MB blocks with replication factor 2. Running `hdfs fsck /rides/raw -files -blocks -locations` confirms complete block health across all DataNodes with zero missing or corrupt blocks.

### 5.6 MAPREDUCE JOB 2: ZONE-HOUR AGGREGATION
Job 2 aggregates demand and supply metrics. The mapper emits keys formatted as `pickup_zone|daytype|hour|weather` paired with a 9-element metric vector: `[requests, completed, no_driver, cancelled, wait_sec_sum, long_waits, drivers_sum, fare_sum, surge_sum]`. `sum_reducer.py` computes associative sums on both the map side (as Combiner) and the reduce side, shrinking the shuffle volume from 1.5 GB to under 12 MB.

### 5.7 MAPREDUCE JOB 3: MULTI-FAMILY SITUATIONAL CUBE
Job 3 constructs a multi-dimensional analytical cube in a single pass. For each ride, the mapper emits 7 distinct key families: `Y|year`, `YZ|year|zone`, `H|daytype|hour`, `DO|daytype|hour`, `W|weather|hour`, `F|festival|hour`, `D|date`, and `M|year-month`. The combiner collapses these vectors locally, generating 1,112 compact analytical summary rows.

### 5.8 MAPREDUCE JOB 4: TOP ROUTES WITH CUSTOM PARTITIONER
Job 4 determines dominant travel corridors across 5 time bands (night, morning, midday, evening, late). Keys are structured as `origin|destination|band`. By configuring `KeyFieldBasedPartitioner` with `-D mapreduce.map.output.key.field.separator='|' -D mapreduce.partition.keypartitioner.options=-k1,1`, Hadoop routes all paths originating from the same zone to the same reducer. `j4_routes_reducer.py` buffers routes and outputs the top 6 destinations per origin.

### 5.9 MAPREDUCE JOB 5: DRIVER SHORTAGE INDEX ALGORITHM
Job 5 reads Job 2 outputs and scores driver shortage on a 0–100 scale using the following formula:
$$\text{Shortage Index} = 45 \times \min\left(\frac{\text{no\_driver\_rate}}{0.25}, 1.0\right) + 35 \times \min\left(\frac{\max(\text{avg\_wait} - 3, 0)}{12}, 1.0\right) + 20 \times \max\left(0, 1.0 - \frac{\text{drivers\_nearby}}{4.0}\right)$$

Area-hours scoring $\ge 50$ are classified as `need_drivers`. Areas with drivers $\ge 7$, wait $< 4.5$ min, and unfulfilled rate $< 3\%$ are classified as `surplus`. All other instances are labeled `balanced`.

### 5.10 MAPREDUCE JOB 6: TOTAL-ORDER SORT HOTSPOT RANKING
Job 6 reads Job 5 records and extracts overall ('all') weather rows. It maps the shortage index into a zero-padded inverted sort key: `1000.0 - ShortageIndex`. Emitting this key with a single reducer (`cat`) leverages Hadoop's native shuffle-sort mechanism to produce a globally sorted, total-order ranking of all 1,440 area-hours without custom sorting code.

### 5.11 INTERACTIVE WEB DASHBOARD IMPLEMENTATION
The dashboard combines semantic HTML, responsive CSS, Leaflet maps, and Chart.js. `export_results.py` packages all TSV files into a unified JSON object (`data.js`). The UI features glowing zone pulses, an interactive 24-hour time slider, weather toggles, tabular zone listings, and a wait-time heatmap.

---

## 6. RESULTS AND DISCUSSION

### 6.1 APPLICATION OUTPUT
Upon completing pipeline execution, the interactive web dashboard provides complete visibility into citywide mobility patterns across all 30 zones of Coimbatore.

### 6.2 DEMAND AND DRIVER SUPPLY ANALYTICS
Analysis of the 19.7 million rides confirms significant temporal and spatial concentration across Coimbatore:

| Performance Metric | Observed Analytical Value |
|---|---|
| **Total Ride Requests (2024 / 2025)** | 9,270,586 in 2024 / 10,361,055 in 2025 (+11.8% growth) |
| **Overall Unfulfilled (No-Driver) Rate** | 6.8% in 2024, rising to 7.5% in 2025 |
| **Citywide Average Wait Time (2025)** | 5 minutes 27 seconds (up from 5m 15s in 2024) |
| **Peak Demand Window** | 6:00 PM – 8:00 PM weekdays (~2,100 requests/hr at 6 PM) |
| **Midday Fleet State (1:00 PM weekday)** | 26 of 30 areas exhibit driver surplus (idle drivers >= 7) |
| **Evening Peak State (6:00 PM weekday)** | 7 acute shortage hotspots concentrated in IT and transit corridors |

### 6.3 WAIT-TIME HEATMAP ANALYTICS
Wait times vary from 2.5 minutes in central areas at midday to over 15 minutes during shift changes. IT zones (Peelamedu, Saravanampatti, Kalapatti, Neelambur) exhibit intense red bands between 5 PM and 8 PM, whereas residential zones remain calm.

### 6.4 WEATHER AND FESTIVAL IMPACT ANALYTICS
In rain conditions, citywide average wait time increases from 5.2 minutes to 8.3 minutes (+61%), while unfulfilled requests jump from 6.4% to 17.9%. During major festivals, unfulfilled rates spike severely: New Year's Eve (28.3%), Deepavali (19.8%), and Pongal (16.2%).

### 6.5 TOP SHORTAGE HOTSPOTS AND ROUTE RANKING
Job 6 ranked all 1,440 area-hours. The top acute shortage hotspots are:

| Zone Name | Day Type | Hour | Shortage Index | No-Driver Rate |
|---|---|---|---|---|
| **Airport** | Weekend | 23:00 | **98.0 / 100** | 41.2% (22.1m wait) |
| **Saravanampatti** | Weekday | 18:00 | **85.0 / 100** | 30.6% (12.2m wait) |
| **Peelamedu** | Weekday | 18:00 | **84.0 / 100** | 29.4% (11.9m wait) |
| **Neelambur** | Weekday | 18:00 | **80.0 / 100** | 27.2% (11.0m wait) |
| **Kalapatti** | Weekday | 18:00 | **79.0 / 100** | 25.6% (10.8m wait) |

### 6.6 HDFS STORAGE AND REPLICATION OUTPUT
Raw data occupies 3,178,007,045 bytes (~3.18 GB) across 24 monthly files. With replication factor 2, total cluster HDFS storage is 6.36 GB distributed across `worker1`, `worker2`, and `worker3`. Cleaned data in `/rides/clean` occupies 3,166,261,514 bytes. HDFS health reports confirm 100% block integrity with zero under-replicated blocks.

### 6.7 YARN EXECUTION AND COUNTER METRICS
All 6 MapReduce jobs completed successfully under YARN with final status SUCCEEDED. Job 1 validated 19,709,930 rows, logging 78,289 dropped rows via custom `DataQuality` counters (`BadCoords: 21,410`, `NegativeFare: 18,920`, `PickupBeforeRequest: 19,810`, `BadStatus: 18,149`). Cleaned row throughput averaged 185,000 rows/second across the 3 worker nodes.

### 6.8 HADOOP BENCHMARKS AND PERFORMANCE EVALUATION
Three benchmark suites were executed via `benchmark.sh`:
* **Reducer Scalability (Job 2):** Execution times with 1, 2, 4, and 8 reducers were 184s, 102s, 58s, and 52s respectively. Scaling from 1 to 4 reducers yielded a 3.17x speedup; adding 8 reducers showed diminishing returns due to shuffle scheduling overhead.
* **Combiner Efficiency (Job 2 & 3):** Running Job 2 with Combiner OFF transmitted 1,480 MB through the network shuffle; enabling the Combiner reduced shuffle I/O to 11.8 MB (92.0% bandwidth savings). In Job 3, shuffle bytes dropped from 1,820 MB to 142 MB (92.2% reduction).
* **Data Scaling:** Job 2 runtimes scaled linearly across dataset sizes: 3 months (7.4s), 6 months (14.2s), 12 months (28.1s), and 24 months (58.4s), verifying predictable $O(N)$ distributed processing.

### 6.9 TESTING AND VALIDATION
| Scenario | Expected Test Outcome | Verification Status |
|---|---|---|
| **HDFS Cluster Ingestion** | 24 CSV files ingested with replication 2 across 3 DataNodes | Verified (fsck clean) |
| **J1 Anomaly Cleansing** | 78,289 corrupt rows filtered, reported via DataQuality counters | Verified in demonstration |
| **J2 Combiner Efficiency** | >85% reduction in shuffle network transmission bytes | Verified (92.0% reduction) |
| **J3 Situational Cubing** | 1,112 compact multi-family summary rows generated | Verified in demonstration |
| **J4 Route Partitioning** | All origin corridors routed to designated reducer | Verified in demonstration |
| **J5 Shortage Scoring** | Accurate 0–100 shortage indices and categorical status labels | Verified in demonstration |
| **J6 Total-Order Ranking** | Globally sorted hotspot ranking produced via shuffle sort | Verified in demonstration |
| **Dashboard Rendering** | Leaflet maps, heatmaps, and charts update in real-time | Verified in demonstration |

---

## 7. CONCLUSION AND FUTURE ENHANCEMENTS

### 7.1 CONCLUSION
The Coimbatore Ride Pulse project demonstrates that distributed computing frameworks like Apache Hadoop can transform high-volume urban ride-hailing logs into granular, actionable dispatch plans. By deploying a 4-node cluster inside Docker, 19.7 million ride records were cleaned, aggregated, and ranked across 30 zones of Coimbatore. The 6-stage MapReduce pipeline highlights the effective use of map-only filtering, associative combiners, custom partitioners, job chaining, and shuffle-based total-order sorting. The findings show that driver shortages are highly localized in IT corridors in the evening and at the airport late at night, providing transportation planners with clear operational guidance.

### 7.2 ADVANTAGES
* **Enterprise-Scale Processing:** Seamlessly processes multi-gigabyte datasets without desktop memory constraints.
* **Resilient Storage:** Block-level HDFS replication ensures high availability and zero data loss.
* **Optimized Shuffle Network:** Associative combiners achieve over 92% reduction in shuffle bandwidth.
* **Specialized Pipeline Modularity:** Independent chained jobs allow isolated testing and tuning.
* **Actionable Decision Support:** Clear visual dashboard enables proactive driver repositioning.

### 7.3 LIMITATIONS
* **Synthetic Ride Data:** Although calibrated against realistic transit dynamics, data is simulated.
* **Batch Latency:** MapReduce is designed for high-throughput batch workloads rather than sub-second real-time streaming.
* **Single-Host Containerization:** The 4-node cluster runs inside Docker on a single laptop host.
* **Fixed Shortage Heuristic:** Shortage index weights are pre-configured rather than dynamically learned.

### 7.4 FUTURE ENHANCEMENTS
* **Cloud Multi-Node Deployment:** Deploying across AWS EMR or Google Cloud Dataproc on bare-metal virtual instances.
* **Real-time Stream Analytics:** Integrating Apache Kafka and Apache Spark Streaming for sub-second live GPS tracking.
* **Machine Learning Demand Forecasting:** Implementing LSTM and XGBoost models to forecast shortages 60 minutes in advance.
* **Dynamic Repositioning Engine:** Automated driver dispatch suggestions with live mobile push notifications.

---

## REFERENCES

[1] J. Dean and S. Ghemawat, “MapReduce: Simplified Data Processing on Large Clusters,” in *Proc. 6th Symposium on Operating Systems Design and Implementation (OSDI)*, 2004.  
[2] Apache Software Foundation, “HDFS Architecture Guide,” *Apache Hadoop 3.3.6 Documentation*, hadoop.apache.org, 2023.  
[3] Apache Software Foundation, “Hadoop Streaming Programming Guide,” *Apache Hadoop 3.3.6 Documentation*, hadoop.apache.org, 2023.  
[4] Apache Software Foundation, “Apache Hadoop YARN Architecture,” *Apache Hadoop 3.3.6 Documentation*, hadoop.apache.org, 2023.  
[5] T. White, *Hadoop: The Definitive Guide: Storage and Analysis at Internet Scale*, 4th ed., O'Reilly Media, 2015.  
[6] Docker Inc., “Docker Compose Specification and Multi-Container Orchestration,” docs.docker.com, 2023.  
[7] Leaflet Contributors, “Leaflet: An Open-Source JavaScript Library for Mobile-Friendly Interactive Maps,” leafletjs.com, 2023.  
[8] Chart.js Contributors, “Chart.js: Simple yet Flexible JavaScript Charting for Designers & Developers,” chartjs.org, 2023.  

---

## APPENDIX

### A. SAMPLE RUN DETAILS
| Configuration / Metric Item | Measured Operational Value |
|---|---|
| **Dataset Time Horizon** | 2024-01-01 to 2025-12-31 (731 days: 523 weekdays, 208 weekends) |
| **Total Raw Requests Processed** | 19,709,930 ride requests (~3.18 GB across 24 CSVs) |
| **Cleansed Valid Records (Job 1)** | 19,631,641 valid rows written to `/rides/clean` |
| **Data Quality Rows Filtered** | 78,289 rows (0.40% error rate: GPS, fare, clock-skew, status) |
| **HDFS Cluster Storage Allocation** | 4-node cluster, 128 MB blocks, replication 2 (6.36 GB total) |
| **YARN Processing Daemons** | 1 ResourceManager on Master, 3 NodeManagers (2 GB RAM each) |
| **Overall 2025 Average Wait Time** | 5 minutes 27 seconds across all 30 zones |
| **Rain vs Dry Wait Time Disparity** | 5.2 minutes in clear weather vs 8.3 minutes during rainfall (+61%) |
| **Peak Unfulfilled Demand Hotspot** | Airport, weekend 11:00 PM (Index: 98.0, 41.2% no-driver rate) |
| **Primary Evening Shortage Corridors** | Saravanampatti, Peelamedu, Neelambur, Kalapatti, Gandhipuram |
| **Midday Surplus Distribution** | 26 of 30 zones record idle drivers >= 7 at 1:00 PM |
| **Combiner Network Reduction** | 1,480 MB reduced to 11.8 MB (92.0% shuffle traffic reduction) |

### B. VERIFICATION COMMANDS
| Hadoop & Docker Command | Operational Verification Purpose |
|---|---|
| `docker compose ps` | Verifies that master, worker1, worker2, and worker3 are active |
| `hdfs dfsadmin -report` | Displays live DataNodes, raw capacity, and DFS used percentage |
| `hdfs fsck /rides/raw -files -blocks` | Verifies HDFS block health, replication status, and block locations |
| `hdfs dfs -ls -R /rides` | Recursively lists raw, clean, and analytical output partitions in HDFS |
| `yarn node -list` | Confirms that all 3 NodeManagers are in RUNNING state |
| `yarn application -list -appStates ALL` | Audits historical YARN application IDs and completion states |
| `mapred job -history /rides/out/...` | Inspects task counter groups, map/reduce counts, and execution times |
