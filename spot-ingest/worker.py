#!/bin/env python

import argparse
import os
import json
import logging
import sys
from common.utils import Util
from common.kerberos import Kerberos
from common.kafka_client import KafkaConsumer

conf_file = '/etc/spot.conf'
worker_conf = ConfigParser.SafeConfigParser()
worker_conf.read(conf_file)

def main():

    # input parameters
    parser = argparse.ArgumentParser(description="Worker Ingest Framework")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (Pipeline Configuration)',metavar='')
    parser.add_argument('-i','--id',dest='id',required=True,help='Worker Id, this is needed to sync Kafka and Ingest framework (Partition Number)',metavar='')
    parser.add_argument('-top','--topic',dest='topic',required=True,help='Topic to read from.',metavar="")
    parser.add_argument('-p','--processingParallelism',dest='processes',required=False,help='Processing Parallelism',metavar="")
    args = parser.parse_args()

    # start worker based on the type.
    start_worker(args.type,args.topic,args.id,args.processes)


def start_worker(type,topic,id,processes=None):

    logger = Util.get_logger("SPOT.INGEST.WORKER")

    # validate the given configuration exists in spot.conf
    try:
        master_conf.get(type, 'type'
    except ConfigParser.NoSectionError:
        logger.error("'{0}' type is not a valid configuration.".format(type))
        sys.exit(1)

    # validate the type is a valid module.
    if not Util.validate_data_source(master_conf.get(type, "type"):
        logger.error("'{0}' type is not configured. Please check /etc/spot.conf".format(type);
        sys.exit(1)

    # validate if kerberos authentication is required.
    if master_conf.get('kerberos', 'KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()

    # create a worker instance based on the data source type.
    module = __import__("pipelines.{0}.worker".format(worker_conf(type, 'type')),fromlist=['Worker'])

    # kafka server info.
    logger.info("Initializing kafka instance")
    k_server = worker_conf.get('ingest', 'kafka_server')
    k_port = worker_conf.get('ingest', 'kafka_port')

    # required zookeeper info.
    zk_server = worker_conf.get('ingest', 'zookeper_server')
    zk_port = worker_conf('ingest','zookeper_port')
    topic = topic

    # create kafka consumer.
    kafka_consumer = KafkaConsumer(topic,k_server,k_port,zk_server,zk_port,id)

    # start worker.
    db_name = worker_conf.get('database','DBNAME']
    app_path = worker_conf.get('ingest', 'hdfs_app_path')
    ingest_worker = module.Worker(db_name,app_path,kafka_consumer,type,processes)
    ingest_worker.start()

if __name__=='__main__':
    main()

