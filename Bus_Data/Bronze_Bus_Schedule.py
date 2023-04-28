#from ENV_Setup import config
#from ENV_Setup import engine
from ENV_Setup import *


busRoutes = pd.read_sql_query('SELECT * FROM "Bus_Routes"', con = engine)

routeId = busRoutes.drop_duplicates(subset=['RouteID'])

def get_bus_schedule(APIKey, routeId):
    #Delete existing SQL table
    #Connect to postgreSQL
    SQLConn = psycopg2.connect(database = config['postgreSQL']['database'],
                               host = config['postgreSQL']['host'],
                               user = config['postgreSQL']['user'],
                               password = config['postgreSQL']['password'],
                               port = config['postgreSQL']['port'])
    SQLConn.autocommit = True
    cursor = SQLConn.cursor()
    sql = f''' DROP TABLE IF EXISTS public."Route_{route}"''';
    cursor.execute(sql)
    SQLConn.close()

    #Set API Key
    headers = {
        # Request headers
        'api_key': f'{APIKey}',
        }
    
    #Set requested parameters    
    params = urllib.parse.urlencode({
        # Request parameters
        'RouteID': f'{routeId}',
        'Date': '',
        'IncludingVariations': 'false'
        })
    
    #Retrieve data
    try:
        conn = http.client.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jRouteSchedule?%s" % params, "{body}", headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        if data == b'{"Message":"Pattern/route name not specified, invalid, or does not exist in this schedule."}':
            print(f'Route_{routeId} does not exist')
            return
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))


    #Convert data to dict format
    jsonData = json.loads(data)
    #Convert data to pandas dataframe for the first direction.
    busRoute0 = pd.DataFrame.from_dict(jsonData['Direction0'])
    #Convert data to pandas dataframe for the second direction. If the bus only moves in one direction (i.e. a loop) then this will be null
    busRoute1 = pd.DataFrame.from_dict(jsonData['Direction1'])

    #Save data to SQL
    if pd.isnull(busRoute0).all().all():
        madeTable = False
    else:
        busRoute0 = busRoute0.set_index(['TripID'])
        busRoute0['StopTimes'] = busRoute0['StopTimes'].apply(lambda row: str(row))
        busRoute0.to_sql(f'Route_{routeId}', 
             con = engine,
             if_exists = 'replace', #Future version should create a test to append this data rather than replace it.
             index = False)
        madeTable = True

    if pd.isnull(busRoute1).all().all():
        pass
    #If the table has data from the opposite direction.
    elif madeTable == True:
        busRoute1 = busRoute1.set_index('TripID')
        busRoute1['StopTimes'] = busRoute1['StopTimes'].apply(lambda row: str(row))
        busRoute1.to_sql(f'Route_{routeId}', 
             con = engine,
             if_exists = 'append', 
             index = False)
    #If there is no existing table (i.e. the route is a loop).    
    else:
        busRoute1 = busRoute1.set_index('TripID')
        busRoute1['StopTimes'] = busRoute1['StopTimes'].apply(lambda row: str(row))
        busRoute1.to_sql(f'Route_{routeId}', 
             con = engine,
             if_exists = 'replace', #Future version should create a test to append this data rather than replace it.
             index = False)
    print(f'Route_{routeId} is done.')
   
   

APIKey = config['WMATA']['API_Key']

for route in routeId['RouteID']:
    get_bus_schedule(APIKey, route)
