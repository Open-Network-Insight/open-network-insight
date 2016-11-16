import os
import logging
import subprocess
import ConfigParser

def main():

    #initialize logging
    logger = get_logger('SPOT.HDFS.SETUP',create_file=False)

    #initialize ConfigParser
    conf_file = '/etc/spot.conf'
    conf = ConfigParser.SafeConfigParser()
    spot_conf = conf.read(conf_file)
    #check for file
    if len(spot_conf) < 1:
        logger.info("Failed to open /etc/spot.conf, check file location and try again")
        raise SystemExit

    #Get configuration
    DSOURCES = conf.get('DEFAULT','DSOURCES').split()
    DFOLDERS = conf.get('DEFAULT','DFOLDERS').split()
    HUSER = conf.get('DEFAULT','HUSER')
    USER = os.environ.get('USER')
    DBNAME = conf.get('DATABASE','DBNAME')

    #create hdfs folders
    mkdir = "sudo -u hdfs hadoop fs -mkdir " + HUSER
    execute_cmd(mkdir,logger)
    chown = "sudo -u hdfs hadoop fs -chown " + USER + ":supergroup " + HUSER
    execute_cmd(chown,logger)

    for source in DSOURCES:
        cmd = "hadoop fs -mkdir {0}/{1}".format(HUSER,source)
        execute_cmd(cmd,logger)
        for folder in DFOLDERS:
            cmd = "hadoop fs -mkdir {0}/{1}/{2}".format(HUSER,source,folder)
            execute_cmd(cmd,logger)

    #Create hive tables
    #create catalog
    cmd = "hive -e 'CREATE DATABASE {0}'".format(DBNAME)
    for source in DSOURCES:
        cmd = "hive -hiveconf huser={0} -hiveconf dbname={1} -f create_{2}_avro_parquet.hql".format(HUSER,DBNAME,source)
        execute_cmd(cmd,logger)

def execute_cmd(command,logger):

    try:
        logger.info("Executing: {0}".format(command))
        subprocess.call(command,shell=True)

    except subprocess.CalledProcessError as e:
        logger.error("There was an error executing: {0}".format(e.cmd))
        sys.exit(1)

def validate_parameter(parameter,message,logger):
    if parameter == None or parameter == "":
        logger.error(message)
        sys.exit(1)

def get_logger(logger_name,create_file=False):

    # create logger for prd_ci
    log = logging.getLogger(logger_name)
    log.setLevel(level=logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if create_file:
            # create file handler for logger.
            fh = logging.FileHandler('SPOT.log')
            fh.setLevel(level=logging.DEBUG)
            fh.setFormatter(formatter)
    # create console handler for logger.
    ch = logging.StreamHandler()
    ch.setLevel(level=logging.DEBUG)
    ch.setFormatter(formatter)

    # add handlers to logger.
    if create_file:
        log.addHandler(fh)

    log.addHandler(ch)
    return  log



if __name__=='__main__':
    main()
