import os.path, yaml

def load_config():
	search_paths = ['/etc/stream_sql/streamsql.yml', './streamsql.yml', '../streamsql.yml']
	for file_loc in search_paths:
		if os.path.isfile(file_loc) == False:
			pass
		else:
			with open(file_loc) as f:
				Config = yaml.safe_load(f)					# use safe_load instead load
				return (Config)
	return {}