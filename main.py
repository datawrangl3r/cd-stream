import os
import logging
from helpers.read_yaml import load_config
from helpers.DBEngines import DBEngine

print (dir(logging))

if __name__ == '__main__':
	config = load_config()

	if config == {}:
		os.sysexit()
	else:
		db = DBEngine(config)
		db_info = db.test_connection()
		logging.warning(db_info)

		if db.test_connection()['status'].upper() == 'SUCCEEDED':
			db.test_replication()
			pass