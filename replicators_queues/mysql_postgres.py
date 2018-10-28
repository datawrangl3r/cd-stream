import os
from replicators_queues.queues import Queues
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, FormatDescriptionEvent, RotateEvent,XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, TableMapEvent

class mysql_postgres:
	def __init__(self, extraction_settings, commit_settings, queue_settings):
		self.resume_file = '/tmp/stream.loc'

		self.mysql_settings = {
			"host": extraction_settings["HOST"],
			"port": extraction_settings["PORT"],
			"user": extraction_settings["USER"],
			"passwd": extraction_settings["PASS"]
		}

		self.queue = Queues(queue_settings)

		if os.path.isfile(self.resume_file) == True:
			self.log_filename, self.log_filepos = open('/tmp/stream.loc','r').read().split('~')
			self.log_filepos = int(self.log_filepos)
		else:
			self.log_filename, self.log_filepos = None,None

		try:
			self.stream = BinLogStreamReader(
						connection_settings=self.mysql_settings,
						server_id=1,
						blocking=True, 
						resume_stream = True,
						only_events = [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent],
						log_pos = self.log_filepos,
						log_file = self.log_filename)

			for binlogevent in self.stream:
				self.log_filename, self.log_pos = [self.stream.log_file, self.stream.log_pos]

				if isinstance(binlogevent, QueryEvent):
					func_name = str(binlogevent.query).split(' ')[0].lower()
					query = str(binlogevent.query)

					if func_name in ['create'] or func_name in ['alter']:
						self.queue.submit_job(func_name, [commit_settings, query])

				elif isinstance(binlogevent, RotateEvent) == False and \
						isinstance(binlogevent, FormatDescriptionEvent) == False and \
						isinstance(binlogevent, TableMapEvent) == False and \
						isinstance(binlogevent, XidEvent) == False:

					for row in binlogevent.rows:
						log_position=binlogevent.packet.log_pos
						table_name=binlogevent.table
						event_time=binlogevent.timestamp
						schema_row = binlogevent.schema
				
						if isinstance(binlogevent, DeleteRowsEvent):
							self.queue.submit_job('delete', [commit_settings, table_name, row["values"]])
						elif isinstance(binlogevent, WriteRowsEvent):
							self.queue.submit_job('insert', [commit_settings, table_name, row["values"]])
						elif isinstance(binlogevent, UpdateRowsEvent):
							self.queue.submit_job('update', [commit_settings, table_name, row["before_values"], row["after_values"]])
						
		except Exception as e:
			self.kill(self.log_filename, self.log_filepos)


	def kill(self, log_filename, log_filepos):
		if log_filename != None and log_filepos != None:
			write_resume_file = open(self.resume_file,'w')
			write_resume_file.write('%s~%s'%(log_filename, log_filepos))
			write_resume_file.close()
		return [log_filename, log_filepos]