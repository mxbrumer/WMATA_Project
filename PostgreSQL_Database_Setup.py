from ENV_Setup import *

database = config['postgreSQL']['database']
host = config['postgreSQL']['host']
user = config['postgreSQL']['user']
password = config['postgreSQL']['password']
port = config['postgreSQL']['port']

SQLConn = psycopg2.connect(database = database,
                           host = host,
                           user = user,
                           password = password,
                           port = port)

SQLConn.autocommit = True


cursor = SQLConn.cursor()

sql = ''' CREATE DATABASE wmata_data ''';

cursor.execute(sql)

SQLConn.close()