import re
from decimal import *
import psycopg2 as pg2

stop_words = {

				" unsigned ": " ",
				" UNSIGNED ": " ",
				" datetime ": " timestamp ",
				" DATETIME ": " TIMESTAMP "
			}

def delete(commit_settings, tablename, value_dict):
    com_db = pg2.connect(
        host	= commit_settings["HOST"],
                        port	 = commit_settings["PORT"],
                        user 	 = commit_settings["USER"],
                        password = commit_settings["PASS"],
                        dbname	 = commit_settings['DB'])

    commit_cursor = com_db.cursor()
    lod_query = "DELETE FROM {} WHERE ".format(tablename)

    for x in value_dict:
        if lod_query.strip().endswith('WHERE') == False:
            lod_query += " and {} = \'{}\'".format(x, value_dict[x])
        else:
            lod_query += " {} = \'{}\'".format(x, value_dict[x])

    lod_query = lod_query.strip()
    commit_cursor.execute(lod_query)
    com_db.commit()
    com_db.close()

def insert(commit_settings, tablename, value_dict):
    com_db = pg2.connect(
                        host	 = commit_settings["HOST"],
                        port	 = commit_settings["PORT"],
                        user 	 = commit_settings["USER"],
                        password = commit_settings["PASS"],
                        dbname	 = commit_settings['DB'])

    commit_cursor = com_db.cursor()
    col_names  = []
    col_values = []

    for x in value_dict:
        col_names.append(x)
        if value_dict[x] is None or value_dict[x] is 'None':
            col_names.remove(x)
        else:
            col_values.append(str(value_dict[x]))

    lod_query = "INSERT INTO {} ({}) VALUES({}".format(tablename, str(col_names)[1:-1].replace("'" ,"")
                                                       ,'%s,  ' *len(col_values))
    lod_query = lod_query.strip()[:-1 ] +')'
    commit_cursor.execute(lod_query, col_values)
    com_db.commit()
    com_db.close()

def update(commit_settings, tablename, before_values, after_values):
    com_db = pg2.connect(
                        host	 = commit_settings["HOST"],
                        port	 = commit_settings["PORT"],
                        user 	 = commit_settings["USER"],
                        password = commit_settings["PASS"],
                        dbname	 = commit_settings['DB'])

    diff_dict = {x :before_values[x] for x in before_values if before_values[x] != after_values[x]}
    cond_dict = {x :before_values[x] for x in before_values if before_values[x] == after_values[x]}

    commit_cursor = com_db.cursor()
    lod_query = "UPDATE {} SET".format(tablename)

    for x in diff_dict:
        lod_query += " {} = \'{}\', ".format(x, diff_dict[x])

    lod_query = lod_query.strip()[:-1] + " WHERE"

    for x in cond_dict:
        if lod_query.strip().endswith('WHERE') == False:
            lod_query += " and {} = \'{}\'".format(x, cond_dict[x])
        else:
            lod_query += " {} = \'{}\'".format(x, cond_dict[x])

    lod_query = lod_query.strip()
    commit_cursor.execute(lod_query)
    com_db.commit()
    com_db.close()

def create(commit_settings, query):
    com_db = pg2.connect(
                        host 	 = commit_settings["HOST"],
                        port 	 = commit_settings["PORT"],
                        user 	 = commit_settings["USER"],
                        password = commit_settings["PASS"],
                        dbname	 = commit_settings['DB'])
    for each_stop_word in stop_words:
        query = re.sub(each_stop_word, stop_words[each_stop_word], query)
    commit_cursor = com_db.cursor()
    commit_cursor.execute(query)
    com_db.commit()
    com_db.close()

def alter(commit_settings, query):
	com_db = pg2.connect(
						host 	 = commit_settings["HOST"],
						port 	 = commit_settings["PORT"],
						user 	 = commit_settings["USER"],
						password = commit_settings["PASS"],
						dbname	 = commit_settings['DB'])
	for each_stop_word in stop_words:
		query = re.sub(each_stop_word, stop_words[each_stop_word], query)
	commit_cursor = com_db.cursor()
	commit_cursor.execute(query)
	com_db.commit()
	com_db.close()
