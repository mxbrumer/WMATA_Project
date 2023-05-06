from ENV_Setup import *
from Postgres_Functions import *
from Bus_Schedule_Functions import *

#Pull list of routes
busRoutes = pd.read_sql_query('SELECT * FROM "Bus_Routes"', con = engine)
routeId = busRoutes.drop_duplicates(subset=['RouteID'])

#Create bronze level route tables and save to postgres########################################################################################################################
for route in routeId['RouteID']:

    # Set API key and postgres information
    APIKey = config['WMATA']['API_Key']
    schema = 'bronze'
    table = f'Route_{route}'
    database = f"{config['postgreSQL']['database']}"
    host = config['postgreSQL']['host']
    user = config['postgreSQL']['user']
    password = config['postgreSQL']['password']
    port = config['postgreSQL']['port']

    # Pipeline
    drop_existing_postgres_table(schema = schema,
                                 table = table, 
                                 dtb = database, 
                                 hst = host, 
                                 usr = user, 
                                 pswd = password, 
                                 prt = port)

    
    jsonData = pull_bus_schedule_from_api(APIKey, 
                                          routeId = route, 
                                          date = '', 
                                          variations = False)

    
    busRoute0, busRoute1 = convert_json_data_to_pandas(jsonData)


    save_bus_schedule_data_to_postgres(schema, table, busRoute0, busRoute1)

    print(f'{table} is done')



#Create silver level tables###################################################################################################################################################


#Create gold level tables#####################################################################################################################################################

