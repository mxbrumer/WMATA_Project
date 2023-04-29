from ENV_Setup import *

#Create function for pulling data.
def bus_position_pull(APIKey, 
                      RouteID = '',
                      Lat = '',
                      Lon = '',
                      Radius = ''):
    headers = {
        # Request headers
        'api_key': APIKey
    }

    params = urllib.parse.urlencode({
        # Request parameters. All paramaters are left blank in order to collect data from all buses. All parameters are optional.
        'RouteID': '',
        'Lat': '',
        'Lon': '',
        'Radius': ''
    })

    try:
        conn = http.client.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jBusPositions?%s" % params, "{body}", headers)
        response = conn.getresponse()
        data = response.read()
        #print(data)
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))

    jsonData = json.loads(data)
    busPositions = pd.DataFrame.from_dict(jsonData['BusPositions'])

    return busPositions


#Create a function to check the time. Is the current time between a start time and end time?
def check_time(begin_time, end_time, time=None):
    # If check time is not given, default to current UTC time
    time = time or datetime.utcnow().time()
    if begin_time < end_time:
        return time >= begin_time and time <= end_time
    else: # crosses midnight
        return time >= begin_time or time <= end_time
    

#Create a function for creating a Bus_Positions table in postgreSQL
def bus_position_create(busPositions):
     busPositions.to_sql('Bus_Positions', 
                         con = engine,
                         if_exists = 'replace',
                         index = False)

#Create a function for deleting the Bus_Positions function at the end of the day.
def bus_position_delete(Bus_Positions):
    SQLConn = psycopg2.connect(database = config['postgreSQL']['database'],
                               host = config['postgreSQL']['host'],
                               user = config['postgreSQL']['user'],
                               password = config['postgreSQL']['password'],
                               port = config['postgreSQL']['port'])
    SQLConn.autocommit = True
    cursor = SQLConn.cursor()
    sql = f''' DROP TABLE IF EXISTS public."{Bus_Positions}"''';
    cursor.execute(sql)
    SQLConn.close()

#Create a function for appending the Bus_Positions table through out the day.
def bus_position_append(busPositions):
        busPositions.to_sql('Bus_Positions', 
             con = engine,
             if_exists = 'append',
             index = False)
