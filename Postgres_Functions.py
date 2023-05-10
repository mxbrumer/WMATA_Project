from ENV_Setup import *

# Drop an existing table from the postgres database. 
# Must specify the desired database name.
# Must specify the postgres database (dbt), host (hst), user (usr), password (pswd), and port (prt). 
# Returns nothing.
def create_postgres_database(databaseName, dtb, hst, usr, pswd, prt):
    #Connect to postgreSQL
    SQLConn = psycopg2.connect(database = dtb,
                               host = hst,
                               user = usr,
                               password = pswd,
                               port = prt)
    SQLConn.autocommit = True
    cursor = SQLConn.cursor()
    sql = f''' CREATE DATABASE {databaseName} ''';
    cursor.execute(sql)
    SQLConn.close()


# Drop an existing table from the postgres database. 
# Must specify the postgres schmema and table.
# Must specify the postgres database (dbt), host (hst), user (usr), password (pswd), and port (prt). 
# Returns nothing.
def drop_existing_postgres_table(schema, table, dtb, hst, usr, pswd, prt):
    #Connect to postgreSQL
    SQLConn = psycopg2.connect(database = dtb,
                               host = hst,
                               user = usr,
                               password = pswd,
                               port = prt)
    SQLConn.autocommit = True
    cursor = SQLConn.cursor()
    sql = f'DROP TABLE IF EXISTS "{schema}"."{table}";'
    cursor.execute(sql)
    SQLConn.close()

def retireve_sql_table(schema, table):
    try:
        pandasTable = pd.read_sql_query(f'SELECT * FROM {schema}."{table}"', con = engine)
        return pandasTable
    except:
        return 'Table Does Not Exist'

# Save pandas DataFrames to the postgres database. 
# Must specify pandas DataFrames, postgres schema, and table
# Returns nothing. 
def save_bus_schedule_data_to_postgres(schema, table, busRoute0, busRoute1):
    # Save data for the first direction.
    # Check if both directions are null (the schedule does not exist).
    if pd.isnull(busRoute0).all().all() and pd.isnull(busRoute1).all().all():
        return
    
    # Check to see if there is data for the first derection. If there is data, a table will be created. if not, madeTable will be set to False.
    if pd.isnull(busRoute0).all().all():
        madeTable = False 
    else:
        busRoute0 = busRoute0.set_index(['TripID'])
        busRoute0['StopTimes'] = busRoute0['StopTimes'].apply(lambda row: str(row))
        busRoute0.to_sql(table, 
             con = engine,
             if_exists = 'replace', #Future version should create a test to append this data rather than replace it.
             schema = schema,
             index = False)
        madeTable = True

    #Save data for the second direction.
    #check to see if there is data for the second derection.
    if pd.isnull(busRoute1).all().all():
        return
    
    #If the table has data from the opposite direction data will be appended to the table created above.
    if madeTable == True:
        busRoute1 = busRoute1.set_index('TripID')
        busRoute1['StopTimes'] = busRoute1['StopTimes'].apply(lambda row: str(row))
        busRoute1.to_sql(table, 
             con = engine,
             if_exists = 'append', #The append argument adds the data to the existing table
             schema = schema,
             index = False)
        
    #If there is no existing table (i.e. the route is a loop) data will be saved to a new table.
    else: #madeTable == False:
        busRoute1 = busRoute1.set_index('TripID')
        busRoute1['StopTimes'] = busRoute1['StopTimes'].apply(lambda row: str(row))
        busRoute1.to_sql(table, 
             con = engine,
             if_exists = 'replace', #Replace drops the existing table if there is one and creates a new table.
             schema = schema,
             index = False)
        
def save_table_to_postgres(pdDataFrame, tableName, schema):
    pdDataFrame.to_sql(tableName, 
             con = engine,
             if_exists = 'replace', #Replace drops the existing table if there is one and creates a new table.
             schema = schema,
             index = False)