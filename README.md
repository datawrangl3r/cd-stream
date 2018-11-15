# CD-Stream

[![V1.0](https://github.com/datawrangl3r/cd-stream/blob/master/cd-stream.png)](https://github.com/datawrangl3r/cd-stream)

CD-Stream is a cross-database CDC driven replicator tool that currently supports replication between MySQL and Postgres.

## The Reason Why:
 - Timed Data extraction (Straight forward ETLs) using selects on a production database can be costly and intensive. 
 - Cron jobs might have to be scheduled and what if they fail too?

## What's New?
In the current version, the support is provided for replication from MySQL and loading the data onto Postgres and new . 
The loading jobs are queued in redis and processed automatically; thanks to rq workers.

## Prerequisite:
Check if binary logging is enabled in your source database. Issue the following command in your source database to verify:

**Mysql**:

```sql
select variable_value as "BINARY LOGGING STATUS (log-bin) :: " from information_schema.global_variables where variable_name='log_bin';
```
If the above command returns "OFF", make sure that the following lines are added to the /etc/mysql/mysql.conf.d and restart the mysql service:

```yml
log_bin                 = mysql-bin
expire_logs_days        = 10
max_binlog_size         = 100M
```

Make sure the user which you use in the extraction block, has the replication privileges.
It can be enabled as:

```sql
mysql> CREATE USER 'repl'@'%.example.com' IDENTIFIED BY 'password';
mysql> GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%.example.com';
mysql> FLUSH PRIVILEGES:
```

## All Set.. Time to Wrangle!!
**Safety first - Put your hard hats on !**
1. Clone the project and Initialize a virtual environment. 
```sh
$ git clone https://github.com/datawrangl3r/cd-stream.git
$ cd cd-stream
$ python3 -m venv .
$ source bin/activate
$ pip install -r requirements.txt
```
2. Configure the streamsql.yml - Tailor it based on your needs
```yml
EXTRACTION:
    ENGINE: mysql
    HOST: localhost
    PORT: 3306
    USER: root
    PASS: password
    DB: SOURCEDB
COMMIT:
    ENGINE: postgres
    HOST: localhost
    PORT: 5432
    USER: postgres
    PASS: password
    DB: TARGETDB
QUEUE:
    ENGINE: REDIS
    HOST: localhost
```
3. Initialize rq workers in the background:
```sh
$ rq worker &
```

4. Start Replication and Data Load (Use Supervisor if needed)
```sh
$ python main.py &
```

