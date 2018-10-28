import datetime
from replicators_queues.mysql_postgres_funcs import *

class Queues:
	def __init__(self, queue_settings):
		if queue_settings["ENGINE"].upper() == "REDIS":
			from rq import Queue
			from redis import Redis

			self.rq = Queue(connection=Redis(queue_settings["HOST"]), async=True)
	
	def submit_job(self, function_name, argument_list):
		if len(argument_list) == 4:
			job = eval("""self.rq.enqueue({}, {}, \"{}\", {}, {})""".format(function_name, str(argument_list[0]), str(argument_list[1]), argument_list[2], argument_list[3]))
		elif len(argument_list) == 3:
			job = eval("""self.rq.enqueue({}, {}, \"{}\", {})""".format(function_name, str(argument_list[0]), str(argument_list[1]), argument_list[2]))
		elif len(argument_list) == 2:
			 job = eval("""self.rq.enqueue({}, {}, \"{}\")""".format(function_name, str(argument_list[0]), str(argument_list[1]).replace('\n','')))