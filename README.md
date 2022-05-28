## Data Migration Guide

### 1. Metadata Migration

#### 1.1. DistCp

DistCp (distributed copy) is a tool used for large inter/intra-cluster copying. 

- It uses MapReduce to effect its distribution, error handling and recovery, and reporting.
- It expands a list of files and directories into input to map tasks, each of which will copy a partition of the files specified in the source list.

#### 1.2. Usages

```
bash$ hadoop distcp hdfs://nn1:8020/foo/bar \
hdfs://nn2:8020/bar/foo

```

#### 1.3. Attentions

- If another client is still writing to a source file, the copy will likely fail.
- Attempting to overwrite a file being written at the destination should also fail on HDFS.
- If a source file is moved before it is copied, the copy will fail with a `FileNotFoundException`.

#### 1.4. Sync

##### 1.4.1. Option

- `-diff` option syncs files from a source cluster to a target cluster with a snapshot diff. It copies, renames and removes files in the snapshot diff list.
- `-update` option must be included when `-diff` option in use.

```
bash$ hadoop distcp -update -diff <from_snapshot> <to_snapshot> <source> <destination>

```

##### 1.4.2. Experiment

```
# Create source and destination directories
hdfs dfs -mkdir /src/ /dst/
# Allow snapshot on source
hdfs dfsadmin -allowSnapshot /src/
# Create a snapshot (empty one)
hdfs dfs -createSnapshot /src/ snap1
# Allow snapshot on destination
hdfs dfsadmin -allowSnapshot /dst/
# Create a from_snapshot with the same name
hdfs dfs -createSnapshot /dst/ snap1

# Put one text file under /src/
echo "This is the 1st text file." > 1.txt
hdfs dfs -put 1.txt /src/
# Create the second snapshot
hdfs dfs -createSnapshot /src/ snap2

# Put another text file under /src/
echo "This is the 2nd text file." > 2.txt
hdfs dfs -put 2.txt /src/
# Create the third snapshot
hdfs dfs -createSnapshot /src/ snap3

# The command below should succeed, 1.txt will be copied from /src/ to /dst/.
hadoop distcp -update -diff snap1 snap2 /src/ /dst/

```

#### 1.5. Underlined command line options

- `-log <logdir>`: Write logs to <logdir>.

- `-m <num_maps>`: Maximum number of simultaneous copies.

> Specify the number of maps to copy data. Note that more maps may not necessarily improve throughput.

- `-strategy {dynamic|uniformsize}`: Choose the copy-strategy to be used in DistCp, by default, uniformsize is used.

> - `UniformSizeInputFormat`: The aim of it is to make each map copy roughly the same number of bytes. The listing file is split into groups of paths, such that the sum of file-sizes in each InputSplit is nearly equal to every other map. The trivial implementation keeps the setup-time low.

> - `DynamicInputFormat` and `DynamicRecordReader`: The listing-file is split into several 'chunk-files', the exact number of chunk-files being a multiple of the number of maps requested for in the Hadoop Job. The process continues until no more chunks are available. It allows faster map-tasks to consume more paths than slower ones, thus speeding up the `DistCp` job overall.


#### 1.6. Distcp vs CP

Take the difference between local application. and distributed applicationes into account.

### 2. Data Migration

- Create target database.
- Collect and list all tables in use.
- Create tables overall.

### 3. Non-Migration List

- dw_all.db2_app_adsource_report








