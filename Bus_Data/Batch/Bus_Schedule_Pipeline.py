from ENV_Setup import *
from Postgres_Functions import *
from Bus_Schedule_Functions import *

#Pull list of routes
busRoutes = retireve_sql_table(schema = 'bronze',
                               table = 'busroutes')
routeId = busRoutes.drop_duplicates(subset=['RouteID'])

#Create bronze level route tables and save to postgres########################################################################################################################
for route in routeId['RouteID']:

    # Set API key and postgres information
    APIKey = config['WMATA']['API_Key']
    schema = 'bronze'
    table = f'route{route}'
    database = config['postgreSQL']['database']
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

for route in routeId['RouteID']:

    # Set API key and postgres information
    APIKey = config['WMATA']['API_Key']
    schema = 'silver'
    table = f'route{route}'
    database = config['postgreSQL']['database']
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
    
    routePd = retireve_sql_table(schema = "bronze",
                                 table = table)

    if isinstance(routePd, pd.DataFrame) == False:
        continue
    
    # Seperate data by direction
    routePd0, routePd1 = split_route_by_bus_direction(routePd)
    routePd0 = routePd0.reset_index()
    routePd1 = routePd1.reset_index()


    if pd.isnull(routePd0).all().all() == False:
        stopTimesDicts0 = convert_series_of_strings_to_series_of_dict(routePd0['StopTimes'])

        stopTimesExpandedPd0 = expand_series_of_dicts_to_pandas(stopTimesDicts0)
        stopTimesExpandedPd0['DirectionText'] = routePd0['TripDirectionText'][0]

    if pd.isnull(routePd1).all().all() == False:
        stopTimesDicts1 = convert_series_of_strings_to_series_of_dict(routePd1['StopTimes'])

        stopTimesExpandedPd1 = expand_series_of_dicts_to_pandas(stopTimesDicts1)
        stopTimesExpandedPd1['DirectionText'] = routePd1['TripDirectionText'][0]



    if pd.isnull(routePd0).all().all() and pd.isnull(routePd1).all().all():
        continue

    if pd.isnull(routePd0).all().all() == False and pd.isnull(routePd1).all().all() == True:
        stopTimesExpandedPdFull = stopTimesExpandedPd0

    if pd.isnull(routePd0).all().all() == True and pd.isnull(routePd1).all().all() == False:
        stopTimesExpandedPdFull = stopTimesExpandedPd1

    if pd.isnull(routePd0).all().all() == False and pd.isnull(routePd1).all().all() == False:
        stopTimesExpandedPdFull = pd.concat([stopTimesExpandedPd0, stopTimesExpandedPd1])

    save_table_to_postgres(stopTimesExpandedPdFull, table, schema)

    print(f'{table} is done')


#Create gold level tables#####################################################################################################################################################

