from ENV_Setup import *
from Postgres_Functions import *

database = 'postgres',
host = config['postgreSQL']['host'],
user = config['postgreSQL']['user'],
password = config['postgreSQL']['password'],
port = config['postgreSQL']['port']


# Pipeline
create_postgres_database(databaseName = database, 
                         dbt = database, 
                         hst = host, 
                         usr = user, 
                         pswd = password, 
                         prt = port)