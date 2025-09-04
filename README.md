# Apache Flink Real-Time Project

![Static Badge](https://img.shields.io/badge/Apache-Paimon-blue?logo=apache&logoColor=%23E6526F&labelColor=black)
![Static Badge](https://img.shields.io/badge/Apache-Flink-blue?logo=apache&logoColor=%23E6526F&labelColor=black)

## Modules Here:

1. [x] **`realtime-example`**: A module containing example Flink Java application jobs. Serves as a reference and starting point for developing new data streaming pipelines.
1. [x] **`realtime-common`**: A foundational module providing core components and utilities for real-time data processing at each layer.
   * **`base`**: Contains base classes for streaming execution
     * Streaming Execution Environment setup and configuration
     * Source connector abstractions and implementations
     * Execute triggers execution
   * **`constant`**
     * Define Public Static Final Constant used across the project for configuration and key naming.
   * **`utils`**
     * A collection of common utility functions and helper classes for data manipulation, logging, and other repetitive tasks.
1. [x] **`realtime-ods`** (Operational Data Store): A module for the Operational Data Store layer processing. It ingests raw data from various sources, performs initial transformations, and loads it into the ODS layer for further processing.
1. [x] **`realtime-dwd`** (Data Warehouse Detail): A module for the Data Warehouse Detail layer processing. It refines data from the ODS layer, applies business logic, data cleansing, and dimension normalization. Implemented using SeaTunnel, this module includes examples of User-Defined Functions (UDFs).
1. [x] **`realtime-dwm`** (Data Warehouse Middle): A module for the Data Warehouse Middle layer processing. It aggregates and transforms data from the DWD layer into more structured formats, preparing it for summary and reporting.
1. [x] **`realtime-dws`** (Data Warehouse Summary): A module for the Data Warehouse Summary layer processing. It provides high-level summaries and key performance indicators (KPIs) derived from the DWM layer, optimized for fast query performance.
1. [x] **`realtime-dim`** (Dimension): A module for managing dimension data. It handles the storage, retrieval, and updating of dimension tables used across the data warehouse layers.
1. [x] **`realtime-ads`** (Application Data Service): A module for the Application Data Store layer processing. It serves as the interface for end-user applications, providing curated datasets and APIs for business intelligence and analytics.

### For entering workdir-path faster, using setting alias in bashrc:

Simplify `cd`
```
echo "alias ppf='cd /opt/poc-allin1/native/flink/flink-1.20.1'" >> ~/.bashrc

source ~/.bashrc
```
or Temporary
```
#FLINK_HOME
export FLINK_HOME=/opt/poc-allin1/native/flink/flink-1.20.1
export PATH=$PATH:$FLINK_HOME/bin
```
or Permanent for Single User
```
cat << 'EOF' >> ~/.profile
export FLINK_HOME=/opt/poc-allin1/native/flink/flink-1.20.1
export PATH=$FLINK_HOME/bin:$PATH
EOF

source ~/.profile
```
```
echo "export FLINK_HOME=/opt/poc-allin1/native/flink/flink-1.20.1" >> ~/.profile
echo "export PATH=\$FLINK_HOME/bin:\$PATH" >> ~/.profile
source ~/.profile
```
Test Export Variable
```
env
```

### Submitting Flink Application Job

**Way.1 Using Flink WebUI**

Upload Jar and Specify Destination Main Class

**Way.2 Using Command Line**

SCP Uploading Jar and Use `flink run -c`

realtime-common -> $FLINK_HOME/lib
```shell
scp realtime-common/target/realtime-common-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/lib/common/
```

```
$FLINK_HOME/bin/flink run \
-m localhost:8081 \
-c com.sands.realtime.ods.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods/target/realtime-ods-1.0-SNAPSHOT.jar
```
