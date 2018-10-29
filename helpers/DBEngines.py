## DBs related imports go here
import logging
from replicators_queues.mysql_postgres import mysql_postgres

class DBEngine:
	def __init__(self, config_input):
		try:
			self.extraction_settings = config_input["EXTRACTION"]
			self.commit_settings = config_input["COMMIT"]
			self.queue_settings = config_input["QUEUE"]

			if self.extraction_settings["ENGINE"].upper() == 'MYSQL':
				try:
					self.extraction_settings["HOST"]
				except:
					logging.critical("HOST is unavailable; Considering defaults")
					self.extraction_settings["HOST"] = "localhost"

				try:
					self.extraction_settings["PORT"]
				except:
					logging.critical("PORT is unavailable; Considering defaults")
					self.extraction_settings["PORT"] = ""
				
				try:
					self.extraction_settings["USER"]
				except:
					logging.critical("USER is unavailable; Unable to proceed")
					return {}, {}

				try:
					self.extraction_settings["PASS"]
				except:
					logging.critical("PASS is unavailable; Considering defaults")
					self.extraction_settings["PASS"] = ""

				try:
					self.extraction_settings["ENGINE"]
				except:
					logging.critical("ENGINE is unavailable; Unable to proceed")
					return {}, {}

				try:
					self.extraction_settings["DB"]
				except:
					logging.critical("DB is unavailable; Unable to proceed")
					return {}, {}

				import MySQLdb as mdb

				try:
					self.ext_db = mdb.connect(
								host = self.extraction_settings["HOST"],
								port = self.extraction_settings["PORT"],
								user = self.extraction_settings["USER"],
								passwd = self.extraction_settings["PASS"],
								db = self.extraction_settings["DB"])
				except Exception as e:
					logging.critical("Unable to connect to the Extract Database")

			if self.commit_settings["ENGINE"].upper() == 'POSTGRES':
				try:
					self.commit_settings["HOST"]
				except:
					logging.critical("HOST is unavailable; Considering defaults")
					self.commit_settings["HOST"] = "localhost"

				try:
					self.commit_settings["PORT"]
				except:
					logging.critical("PORT is unavailable; Considering defaults")
					self.commit_settings["PORT"] = ""
				
				try:
					self.commit_settings["USER"]
				except:
					logging.critical("USER is unavailable; Unable to proceed")
					return {}, {}

				try:
					self.commit_settings["PASS"]
				except:
					logging.critical("PASS is unavailable; Considering defaults")
					self.commit_settings["PASS"] = ""

				try:
					self.commit_settings["ENGINE"]
				except:
					logging.critical("ENGINE is unavailable; Unable to proceed")
					return {}, {}

				try:
					self.commit_settings["DB"]
				except:
					logging.critical("DB is unavailable; Unable to proceed")
					return {}, {}

				import psycopg2 as pg2

				try:
					self.com_db = pg2.connect(
								host = self.commit_settings["HOST"],
								port = self.commit_settings["PORT"],
								user = self.commit_settings["USER"],
								password = self.commit_settings["PASS"],
								dbname = self.commit_settings['DB'])
				except:
					logging.critical("Unable to connect to the Commit Database")
				
			if self.queue_settings["ENGINE"].upper() == 'REDIS':
				try:
					self.queue_settings["HOST"]
				except:
					logging.critical("QUEUE-HOST is unavailable; unable to proceed")
					return {}, {}
				
				from redis import Redis

				try:
					Redis(self.queue_settings['HOST'])
				except Exception as e:
					logging.critical("Unable to connect to the Redis Cache")

		except Exception as e:
			return {"status": "Failed", "message": "Oops, Something is not right!!", "error": str(e)}

	def test_connection(self):

		try:
			ext_cur = self.ext_db.cursor()
		except Exception as e:
			return {"status": "Failed", "message": "Extraction Cursor Initiation Failed", "error": str(e)}
			logging.critical("Extraction Cursor not Initiated")
		
		try:
			com_cur = self.com_db.cursor()
		except Exception as e:
			return {"status": "Failed", "message": "Commit Cursor Initiation Failed", "error": str(e)}
			logging.critical("Commit Cursor not Inititated")

		return {"status": "Succeeded", "message": "Commit Cursor Initiation Succeeded"}
	
	def test_replication(self):
		try:
			instance = eval(self.extraction_settings["ENGINE"]+"_"+self.commit_settings["ENGINE"]+'(self.extraction_settings, self.commit_settings, self.queue_settings)')
		except Exception as e:
			return {"status": "Failed", "message": "Replication Initiation Failed", "error": str(e)}
			logging.critical("Replication Tests Failed")
			
