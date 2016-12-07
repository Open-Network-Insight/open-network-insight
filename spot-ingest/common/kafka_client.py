#!/bin/env python
import logging
import os
import sys
from common.utils import Util
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError

# librdkafka kerberos configs
krb_conf_options = {'sasl.mechanisms': 'gssapi',
                    'security.protocol': 'sasl_plaintext',
                    'sasl.kerberos.service.name': 'kafka',
                    'sasl.kerberos.kinit.cmd': 'kinit -k -t "%{sasl.kerberos.keytab}" %{sasl.kerberos.principal}',
                    'sasl.kerberos.principal': os.getenv('KRB_USER'),
                    'sasl.kerberos.keytab': os.getenv('KEYTABPATH'),
                    'sasl.kerberos.min.time.before.relogin': 60000}


class KafkaTopic(object):


    def __init__(self,topic,server,port,zk_server,zk_port,partitions):

        self._initialize_members(topic,server,port,zk_server,zk_port,partitions)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partitions):

        # get logger isinstance
        self._logger = logging.getLogger("SPOT.INGEST.KAFKA")

        # kafka requirements
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._topic = topic
        self._num_of_partitions = partitions
        self._partitions = []
        self._partitioner = None

        # create topic with partitions
        self._create_topic()

    def _create_topic(self):

        self._logger.info("Creating topic: {0} with {1} parititions".format(self._topic,self._num_of_partitions))

        # Create partitions for the workers.
        # self._partitions = [ TopicPartition(self._topic,p) for p in range(int(self._num_of_partitions))]

        # get script path
        zk_conf = "{0}:{1}".format(self._zk_server,self._zk_port)
        create_topic_cmd = "{0}/kafka_topic.sh create {1} {2} {3}".format(os.path.dirname(os.path.abspath(__file__)),self._topic,zk_conf,self._num_of_partitions)

        # execute create topic cmd
        Util.execute_cmd(create_topic_cmd,self._logger)

    @classmethod
    def SendMessage(cls, message, kafka_servers, topic, partition=0):

        producer_conf = {'bootstrap.servers': kafka_servers,
                         'api.version.request': 'false',
                         'broker.version.fallback': '0.9.0.0',
                         'internal.termination.signal': 0}

        if os.getenv('KRB_AUTH'):
            producer_conf.update(krb_conf_options)

        producer = Producer(**producer_conf)
        producer.send(topic,message,partition=partition)
        producer.flush()

    @property
    def Topic(self):
        return self._topic

    @property
    def Partition(self):
        return self._partitioner.partition(self._topic).partition

    @property
    def Zookeeper(self):
        zk = "{0}:{1}".format(self._zk_server,self._zk_port)
        return zk

    @property
    def BootstrapServers(self):
        servers = "{0}:{1}".format(self._server,self._port)
        return servers


class KafkaConsumer(object):

    def __init__(self,topic,server,port,zk_server,zk_port,partition):

        self._initialize_members(topic,server,port,zk_server,zk_port,partition)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partition):

        self._topic = topic
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._id = partition
        self._logger = logging.getLogger("SPOT.INGEST.KAFKA")

    def start(self):

        kafka_brokers = '{0}:{1}'.format(self._server,self._port)
        self._consumer_conf = {'bootstrap.servers': kafka_brokers,
                                'group.id': self._id,
                                'internal.termination.signal': 0,
                                'api.version.request': 'false',
                                'broker.version.fallback': '0.9.0.0'}

        if os.getenv('KRB_AUTH'):
            self._producer_conf.update(krb_conf_options)

        consumer = Consumer(self._consumer_conf)
        subscribed = None

        def on_assign(consumer, partitions):
            self._logger.info('Assigned: {0}, {1}'.format(len(partitions), partitions))
            for p in partitions:
                print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
                p.offset = -1
            consumer.assign(partitions)

        def on_revoke(consumer, partitions):
            self._logger.info('Revoked: {0} {1}'.format(len(partitions), partitions))
            for p in partitions:
                print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
            consumer.unassign()

        running = True

        try:

            consumer.subscribe([self._topic],  on_assign=on_assign, on_revoke=on_revoke)
            self._logger.info('subscribing to ' + self._topic)

            while running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self._logger.info('{0} {1} reached end at offset {2}'.format(msg.topic(), msg.partition(), msg.offset()))
                        continue
                    elif msg.error():
                        self._logger("error: " + msg.error())
                    SystemExit
                else:
                    return msg

        except KeyboardInterrupt:
            self._logger.info('User interrupted')
            raise SystemExit
        except:
            self._logger.info('error: {0}'.format(sys.exc_info()[0]))
            raise SystemExit
        finally:
            self._logger.info('closing down consumer')
            consumer.close()

    @property
    def Topic(self):
        return self._topic

    @property
    def ZookeperServer(self):
        return "{0}:{1}".format(self._zk_server,self._zk_port)
