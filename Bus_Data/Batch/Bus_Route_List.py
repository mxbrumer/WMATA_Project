#Take the poistion of every bus and and extract a list of all bus routes.

from ENV_Setup import *

#Create an API key variable.
APIKey = config['WMATA']['API_Key']

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
pdData = pd.DataFrame.from_dict(jsonData['BusPositions'])

pdData = pdData.drop_duplicates(subset = ['RouteID', 'DirectionText'])

busRoutes = pd.DataFrame(pdData[['RouteID', 'DirectionText', 'TripHeadsign']])

busRoutes.head()

busRoutes['DirectionText'].value_counts()

#Create and add bus route data to Bus_Routes Table

engine = create_engine(f'postgresql://{config["postgreSQL"]["user"]}:{config["postgreSQL"]["password"]}@{config["postgreSQL"]["host"]}:{config["postgreSQL"]["port"]}/{config["postgreSQL"]["database"]}')

busRoutes.to_sql('busroutes', 
                 schema='bronze',
                 con = engine,
                 if_exists = 'replace', #Future version should create a test to append this data rather than replace it.
                 index = False)