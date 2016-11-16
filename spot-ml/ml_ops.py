import os
import logging
import argparse
import subprocess
import ConfigParser

def main():

    #initialize ConfigParser
    conf_file = '../spot-setup/spot.conf'
    conf = ConfigParser.SafeConfigParser()
    spot_conf = conf.read(conf_file)

    #initialize logging
    logger = get_logger('SPOT.ML.OPS',create_file=False)
    
    #check for file
    if len(spot_conf) < 1:
        logger.info("Failed to open /etc/spot.conf, check file location and try again")
        raise SystemExit
    
    ## parse and validate arguments
    tol = None

    # input Parameters
    parser = argparse.ArgumentParser(description="ML Operations script")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be processed',metavar='')
    parser.add_argument('-d','--date',dest='fdate',required=True,help='Specify date to be analyzed by ML',metavar='')
    parser.add_argument('-T','--tol',dest='tol',required=False,help='If present will override default TOL from conf file',metavar='')
    parser.add_argument('-m','--maxResults',dest='MAXRESULTS',required=False,help='If defined sets max results returned',metavar='')
    args = parser.parse_args()
    
    YR=args.fdate[0:4]
    MH=args.fdate[4:6]
    DY=args.fdate[6:8]
    
    #getting defaults for ConfigParser interpolation
    DEFAULTS = vars(args)
    DEFAULTS.update({'YR':YR,'MH':MH,'DY':DY})

    ## prepare parameters pipeline stages
    HPATH = conf.get('DEFAULT','HPATH',vars=DEFAULTS)
    LPATH = conf.get('DEFAULT','LPATH',vars=DEFAULTS)
    MAXRESULTS = conf.get('DEFAULT','MAXRESULTS')
    DUPFACTOR = conf.get('DEFAULT','DUPFACTOR')
    RAWDATA_PATH = conf.get(args.type,'{0}_PATH'.format(args.type.upper()), vars=DEFAULTS)
    FEEDBACK_PATH = "{0}/{1}_scores.csv".format(conf.get('DEFAULT','LPATH',vars=DEFAULTS),args.type)
    PREPROCESS_STEP = "{0}_pre_lda".format(args.type)
    POSTPROCESS_STEP = "{0}_post_lda".format(args.type)
    HDFS_WORDCOUNTS = "{0}/word_counts".format(HPATH)

    ## paths for intermediate files
    HDFS_DOCRESULTS = "{0}/doc_results.csv".format(HPATH)
    LOCAL_DOCRESULTS = "{0}/doc_results.csv".format(LPATH)
    HDFS_WORDRESULTS = "{0}/word_results.csv".format(HPATH)
    LOCAL_WORDRESULTS = "{0}/word_results.csv".format(LPATH)
    
    HDFS_SCORED_CONNECTS = "{0}/scores".format(HPATH)
    
    LDA_OUTPUT_DIR = "{0}/{1}".format(args.type,args.fdate)
    
    #get nodes and create comma seperated list
    NODES = conf.get('DEFAULT','NODES').split()
    nodes_csl = ','.join(NODES)
    
    cmd = "hdfs dfs -rm -R -f {0}".format(HDFS_WORDCOUNTS)
    execute_cmd(cmd,logger)

    cmd = "mkdir -p {0}".format(LPATH)
    execute_cmd(cmd,logger)
     
    # protect the flow_scores.csv file
    cmd = "rm -f {0}/*.{dat,beta,gamma,other,pkl}".format(LPATH)
    execute_cmd(cmd,logger)

    cmd = "hdfs dfs -rm -R -f {0}".format(HDFS_SCORED_CONNECTS)
    execute_cmd(cmd,logger)
    
    # Add -p <command> to execute pre MPI command.
    # Pre MPI command can be configured in /etc/spot.conf
    # In this script, after the line after --mpicmd ${MPI_CMD} add:
    # --mpiprep ${MPI_PREP_CMD}
    
    if conf.get('mpi',"MPI_PREP_CMD"):
        cmd = conf.get('mpi',"MPI_PREP_CMD")
        execute_cmd(cmd,logger)
    
    SPK_CONFIG = conf.get('spark','SPK_CONFIG')
    SPK_DRIVER_MEM = conf.get('spark','SPK_DRIVER_MEM')
    SPK_EXEC = conf.get('spark','SPK_EXEC')
    SPK_EXEC_CORES = conf.get('spark','SPK_EXEC_CORES')
    SPK_EXEC_MEM = conf.get('spark','SPK_EXEC_MEM')
    SPK_EXEC_MEM_OVERHEAD = conf.get('spark', 'SPK_EXEC_MEM_OVERHEAD')
    SPK_DRIVER_MAX_RESULTS = conf.get('spark', 'SPK_DRIVER_MAX_RESULTS')
    SPK_DRIVER_MEM_OVERHEAD = conf.get('spark', 'SPK_DRIVER_MEM_OVERHEAD')
    LDAPATH = conf.get('DEFAULT','LDAPATH')
    LUSER = conf.get('DEFAULT','LUSER')
    MPI_CMD = conf.get('mpi','MPI_CMD')
    PROCESS_COUNT = conf.get('mpi','PROCESS_COUNT')
    TOPIC_COUNT = conf.get('DEFAULT','TOPIC_COUNT')
    
    if tol:
        TOL = tol
    else:
        TOL = conf.get('DEFAULT','TOL')
    
    #prepare options for spark-submit
    spark_cmd = [
        "time", "spark-submit", "--class org.apache.spot.SuspiciousConnects",
         "--master yarn-client", "--conf spark.driver.maxPermSize=512m", "--conf spark.driver.cores=1", 
         "--conf spark.dynamicAllocation.enabled=true", "--conf spark.dynamicAllocation.minExecutors=1", 
         "--conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512M -XX:PermSize=512M", 
         "--conf spark.shuffle.io.preferDirectBufs=false", "--conf spark.kryoserializer.buffer.max=512m", 
         "--conf spark.shuffle.service.enabled=true", "--conf spark.yarn.am.waitTime=1000000"]
    
    spark_extras = [
        "--driver-memory " + SPK_DRIVER_MEM, 
        "--conf spark.dynamicAllocation.maxExecutors=" + SPK_EXEC, 
        "--conf spark.executor.cores=" + SPK_EXEC_CORES, 
        "--conf spark.executor.memory=" + SPK_EXEC_MEM, 
        "--conf spark.driver.maxResultSize=" + SPK_DRIVER_MAX_RESULTS, 
        "--conf spark.yarn.driver.memoryOverhead=" + SPK_DRIVER_MEM_OVERHEAD, 
        "--conf spark.yarn.executor.memoryOverhead=" + SPK_EXEC_MEM_OVERHEAD]
    
    if SPK_CONFIG:
        logger.info('Adding Spark Configurations from spot.conf')
        spark_cmd.extend(spark_extras)

    spot_jar = [         
        "target/scala-2.10/spot-ml-assembly-1.1.jar", 
        "--analysis " + args.type,
        "--input " + RAWDATA_PATH, "--dupfactor " + DUPFACTOR, 
        "--feedback " + FEEDBACK_PATH, 
        "--model " + LPATH + "/model.dat", 
        "--topicdoc " + LPATH + "/final.gamma", 
        "--topicword " + LPATH + "/final.beta", 
        "--lpath " + LPATH, "--ldapath" + LDAPATH, 
        "--luser " + LUSER, "--mpicmd " + MPI_CMD, 
        "--proccount " + PROCESS_COUNT, 
        "--topiccount " + TOPIC_COUNT, 
        "--nodes " + nodes_csl, 
        "--scored " + HDFS_SCORED_CONNECTS, 
        "--threshold " + TOL, 
        "--maxresults " + MAXRESULTS]

    spark_cmd.extend(spot_jar)

    execute_cmd(spark_cmd,logger)
    #process = subprocess.Popen(spark_cmd, stdout=subprocess.PIPE, stderr=None)

    ## move results to hdfs.
    os.chdir(LPATH)
    cmd = "hadoop fs -getmerge {0}/part-* {1}_results.csv && hadoop fs -moveFromLocal {1}_results.csv  {0}/${1}_results.csv".format(HDFS_SCORED_CONNECTS,args.type)
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
    # reate console handler for logger.
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