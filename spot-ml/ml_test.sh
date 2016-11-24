#!/bin/bash

# parse and validate arguments

DSOURCE=$1
RAWDATA_PATH=$2

# read in variables (except for date) from etc/.conf file

source /etc/spot.conf

# third argument if present will override default TOL from conf file

TOL=1.1
MAXRESULTS=20

LPATH=${LUSER}/ml/${DSOURCE}/test
HPATH=${HUSER}/${DSOURCE}/test/scored_results
# prepare parameters pipeline stages


FEEDBACK_PATH=${LPATH}/${DSOURCE}_scores.csv
DUPFACTOR=1000

HDFS_WORDCOUNTS=${HPATH}/word_counts

# paths for intermediate files
HDFS_DOCRESULTS=${HPATH}/doc_results.csv
LOCAL_DOCRESULTS=${LPATH}/doc_results.csv

HDFS_WORDRESULTS=${HPATH}/word_results.csv
LOCAL_WORDRESULTS=${LPATH}/word_results.csv

HDFS_SCORED_CONNECTS=${HPATH}/scores
HDFS_MODEL=${HPATH}/model

LDA_OUTPUT_DIR=test/${DSOURCE}

TOPIC_COUNT=20
LDA_IMP="SparkLDA"

nodes=${NODES[0]}
for n in "${NODES[@]:1}" ; do nodes+=",${n}"; done

hdfs dfs -rm -R -f ${HDFS_WORDCOUNTS}
wait

mkdir -p ${LPATH}
rm -f ${LPATH}/*.{dat,beta,gamma,other,pkl} # protect the flow_scores.csv file

hdfs dfs -rm -R -f ${HDFS_SCORED_CONNECTS}

# Add -p <command> to execute pre MPI command.
# Pre MPI command can be configured in /etc/spot.conf
# In this script, after the line after --mpicmd ${MPI_CMD} add:
# --mpiprep ${MPI_PREP_CMD}

${MPI_PREP_CMD}

time spark-submit --class "org.apache.spot.SuspiciousConnects" \
  --master yarn-client \
  --driver-memory ${SPK_DRIVER_MEM} \
  --conf spark.driver.maxResultSize=${SPK_DRIVER_MAX_RESULTS} \
  --conf spark.driver.maxPermSize=512m \
  --conf spark.driver.cores=1 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=${SPK_EXEC} \
  --conf spark.executor.cores=${SPK_EXEC_CORES} \
  --conf spark.executor.memory=${SPK_EXEC_MEM} \
  --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=512M -XX:PermSize=512M" \
  --conf spark.shuffle.io.preferDirectBufs=false    \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.yarn.am.waitTime=1000000 \
  --conf spark.yarn.driver.memoryOverhead=${SPK_DRIVER_MEM_OVERHEAD} \
  --conf spark.yarn.executor.memoryOverhead=${SPAK_EXEC_MEM_OVERHEAD} target/scala-2.10/spot-ml-assembly-1.1.jar \
  --analysis ${DSOURCE} \
  --input ${RAWDATA_PATH}  \
  --dupfactor ${DUPFACTOR} \
  --feedback ${FEEDBACK_PATH} \
  --model ${LPATH}/model.dat \
  --topicdoc ${LPATH}/final.gamma \
  --topicword ${LPATH}/final.beta \
  --lpath ${LPATH} \
  --ldapath ${LDAPATH} \
  --luser ${LUSER} \
  --mpicmd ${MPI_CMD}  \
  --proccount ${PROCESS_COUNT} \
  --topiccount ${TOPIC_COUNT} \
  --nodes ${nodes} \
  --scored ${HDFS_SCORED_CONNECTS} \
  --tempmodel ${HDFS_MODEL} \
  --threshold ${TOL} \
  --maxresults ${MAXRESULTS} \
  --ldaImplementation ${LDA_IMP} \
  --ldaMaxIterations 11

cd ${LPATH}
hadoop fs -getmerge ${HDFS_SCORED_CONNECTS}/part-* ${DSOURCE}_results.csv && hadoop fs -moveFromLocal \
    ${DSOURCE}_results.csv  ${HDFS_SCORED_CONNECTS}/${DSOURCE}_results.csv