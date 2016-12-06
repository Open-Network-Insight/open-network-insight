import logging
import os
import subprocess
import csv
import sys
import ConfigParser 
import pandas as pd

class Util(object):
	
	@classmethod
	def get_logger(cls,logger_name,create_file=False):
		

		# create logger for prd_ci
		log = logging.getLogger(logger_name)
		log.setLevel(level=logging.INFO)
		
		# create formatter and add it to the handlers
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		
		if create_file:
				# create file handler for logger.
				fh = logging.FileHandler('oa.log')
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

	@classmethod
	def get_spot_conf(cls):
		
		conf_file = "/etc/spot.conf"
		config = ConfigParser.ConfigParser()
		config.readfp(SecHead(open(conf_file)))	

		return config
	
	@classmethod
	def create_oa_folders(cls,type,date):		

		# create date and ingest summary folder structure if they don't' exist.
		root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
		data_type_folder = "{0}/data/{1}/{2}"
		if not os.path.isdir(data_type_folder.format(root_path,type,date)): os.makedirs(data_type_folder.format(root_path,type,date))
		if not os.path.isdir(data_type_folder.format(root_path,type,"ingest_summary")): os.makedirs(data_type_folder.format(root_path,type,"ingest_summary"))

		# create ipynb folders.
		ipynb_folder = "{0}/ipynb/{1}/{2}".format(root_path,type,date)
		if not os.path.isdir(ipynb_folder): os.makedirs(ipynb_folder)

		# retun path to folders.
		data_path = data_type_folder.format(root_path,type,date)
		ingest_path = data_type_folder.format(root_path,type,"ingest_summary")		
		return data_path,ingest_path,ipynb_folder
	
	@classmethod
	def get_ml_results_form_hdfs(cls,hdfs_file_path,local_path):

		# get results from hdfs.
		get_results_cmd = "hadoop fs -get {0} {1}/.".format(hdfs_file_path,local_path)
		subprocess.call(get_results_cmd,shell=True)
		return get_results_cmd

	@classmethod
	def read_results(cls,file,limit, delimiter=','):
		
		# read csv results.
		result_rows = []
		with open(file, 'rb') as results_file:
			csv_reader = csv.reader(results_file, delimiter = delimiter)
			for i in range(0, int(limit)):
				try:
					row = csv_reader.next()
				except StopIteration:
					return result_rows
				result_rows.append(row)
		return result_rows

	@classmethod
	def ip_to_int(self,ip):
		
		try:
			o = map(int, ip.split('.'))
			res = (16777216 * o[0]) + (65536 * o[1]) + (256 * o[2]) + o[3]
			return res    

		except ValueError:
			return None
	
	@classmethod
	def create_csv_file(cls,full_path_file,content,delimiter=','): 

		with open(full_path_file, 'w+') as u_file:
			writer = csv.writer(u_file, quoting=csv.QUOTE_NONE, delimiter=delimiter)
			writer.writerows(content)
	

	@classmethod
	def get_ingest_summary(cls, obj, pipeline):

			# get date parameters.
			yr = obj._date[:4]
			mn = obj._date[4:6]
			dy = obj._date[6:]
			
			ingest_summary_cols = ["date","total"]		
			result_rows = []        

			ingest_summary_file = "{0}/is_{1}{2}.csv".format(obj._ingest_summary_path,yr,mn)			
			ingest_summary_tmp = "{0}/is_{1}{2}.tmp".format(obj._ingest_summary_path,yr,mn)
			df = pd.read_csv(ingest_summary_file, delimiter=',',names=ingest_summary_cols, skiprows=1)
 
			df_filtered = df[df['date'].str.contains("{0}-{1}-{2}".format(yr, mn, dy)) == False] 
			
			# get ingest summary.           
			ingest_summary_qry = ("SELECT tryear, trmonth, trday, trhour, trminute, COUNT(*) total"
								" FROM {0}.{1} "
								" WHERE "
								" y={2} "
								" AND m={3} "
								" AND d={4} "
								" AND unix_tstamp IS NOT NULL "
								" GROUP BY tryear, trmonth, trday, trhour, trminute;")


			ingest_summary_qry = ingest_summary_qry.format(obj._db,pipeline, yr, mn, dy)
			results_file = "{0}/results_{1}.csv".format(obj._ingest_summary_path,obj._date)
			obj._engine.query(ingest_summary_qry,output_file=results_file,delimiter=",")

			with open(results_file, 'rb') as rf:
				csv_reader = csv.reader(rf, delimiter = ",")
				result_rows = list(csv_reader)

			result_rows = iter(result_rows)
			next(result_rows) 
			df_new = pd.DataFrame([["{0}-{1}-{2} {3}:{4}".format(yr, mn, dy, row[3].zfill(2) ,row[4].zfill(2)), row[5]] for row in result_rows],columns = ingest_summary_cols)
			

			df_filtered = df_filtered.append(df_new, ignore_index=True)
 			df_filtered.to_csv(ingest_summary_tmp,sep=',', index=False)
			
			# rm_big_file = "rm {0}".format(results_file)
			# mv_tmp_file = "mv {0} {1}".format(ingest_summary_tmp, ingest_summary_file)
			os.remove(results_file)
			os.rename(ingest_summary_tmp,ingest_summary_file)
		  


class SecHead(object):
    def __init__(self, fp):
        self.fp = fp
        self.sechead = '[conf]\n'

    def readline(self):
        if self.sechead:
            try: 
                return self.sechead
            finally: 
                self.sechead = None
        else: 
            return self.fp.readline()

class ProgressBar(object):

	def __init__(self,total,prefix='',sufix='',decimals=2,barlength=60):

		self._total = total
		self._prefix = prefix
		self._sufix = sufix
		self._decimals = decimals
		self._bar_length = barlength
		self._auto_iteration_status = 0

	def start(self):

		self._move_progress_bar(0)
	
	def update(self,iterator):
		
		self._move_progress_bar(iterator)

	def auto_update(self):

		self._auto_iteration_status += 1		
		self._move_progress_bar(self._auto_iteration_status)
	
	def _move_progress_bar(self,iteration):

		filledLength    = int(round(self._bar_length * iteration / float(self._total)))
		percents        = round(100.00 * (iteration / float(self._total)), self._decimals)
		bar             = '#' * filledLength + '-' * (self._bar_length - filledLength)	
		sys.stdout.write("{0} [{1}] {2}% {3}\r".format(self._prefix, bar, percents, self._sufix))		
		sys.stdout.flush()
		
		if iteration == self._total:print("\n")

		
	

