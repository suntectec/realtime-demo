# Apache Flink Real-Time Project

![Static Badge](https://img.shields.io/badge/Apache-Flink-blue?logo=apache&logoColor=%23E6526F&labelColor=black)
![Static Badge](https://img.shields.io/badge/Apache-Kafka-blue?logo=apache&logoColor=%23E6526F&labelColor=black)
![Static Badge](https://img.shields.io/badge/Apache-Paimon-blue?logo=apache&logoColor=%23E6526F&labelColor=black)

## Modules Here:

1. [x] **`realtime-example`**: 
   * A module containing example Flink Java application jobs. Serves as a reference and starting point for developing new data streaming pipelines.
1. [x] **`realtime-common`**: 
   * A foundational module providing core components and utilities for real-time data processing at each layer.
      * **`base`**: 
        * Contains base class for streaming environment execution
      * **`constant`**
        * Define Public Static Final Constant used across the project for configuration and key naming.
      * **`utils`**
        * A collection of common utility functions and helper classes for data manipulation, logging, and other repetitive tasks.
1. [x] **`realtime-ods`** (Operational Data Store):
   * The ODS layer receives and stages raw data for the data warehouse. The table structures in this layer are identical to those in the source data systems, serving as a staging area for the data warehouse. The ODS layer performs the following operations on raw data:
     * Synchronizes raw structured data to the data warehouse, either incrementally or in full.
     * Structures raw unstructured data, such as log information, and stores it in the data warehouse.
     * Ensures table names in this layer start with ods.
1. [x] **`realtime-dwd`** (Data Warehouse Detail):
   * The DWD layer models business events at the most granular level. You can denormalize data tables by adding key dimension attributes. This practice reduces the need for joins between fact and dimension tables, improving query performance.
1. [x] **`realtime-dws`** (Data Warehouse Summary):
   * The DWS layer builds data models based on the subjects of analysis. It creates public aggregate tables to support upstream metric needs.
   * For example, user behavior from the ODS layer can be pre-classified and aggregated to derive common dimensions such as time, IP address, and ID. This data can be used to calculate metrics, such as the number of products a user purchased from different IP addresses during various time periods.
   * In the DWS layer, you can perform additional lightweight aggregations to improve calculation efficiency. For example, applications can use these daily aggregates to calculate behavior metrics for 7-day, 30-day, and 90-day periods, which can save considerable processing time.
1. [x] **`realtime-dim`** (Dimension): 
   * The DIM layer builds data models using dimensions. Based on actual business needs, it can store dimension tables from logical models or dimension definitions from conceptual models. By defining dimensions, specifying primary keys, adding dimension attributes, and associating different dimensions, you can build consistent enterprise-wide dimension tables for analytics. This helps reduce risks associated with inconsistent data calculation logic and algorithms.
1. [x] **`realtime-ads`** (Application Data Service): 
   * The ADS layer stores custom statistical metrics for data products and is used to generate various reports. For example, an e-commerce company could report on the sales volumes and rankings of various sports balls sold in Singapore between June 9 and 19.

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

### Submitting a Flink Application Job

**Way.1 Using Flink WebUI**

Upload Jar and Specify Destination Main Class

**Way.2 Using Command Line**

SCP Upload jar -> $FLINK_HOME

```shell
scp realtime-common/target/realtime-common-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/lib/common/
```

```shell
scp realtime-ods/target/realtime-ods-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/usrlib/
```

Restart Flink Cluster, when common jar updated to flink/lib.
```
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

Use `flink run -c` to Specify Destination Main Class

```
$FLINK_HOME/bin/flink run -d \
-m localhost:8081 \
-c com.sands.realtime.ods.sqlserver.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-sqlserver-event-data-1.0-SNAPSHOT.jar
```
