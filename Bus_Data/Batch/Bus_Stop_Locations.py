from ENV_Setup import *

#Create an API key variable.
APIKey = config['WMATA']['API_Key']

headers = {
    # Request headers
    'api_key': APIKey
}

#Get requested parameters
params = urllib.parse.urlencode({
    # Request parameters. Left blank to capture all bus stop locations
    'Lat': '{number}',
    'Lon': '{number}',
    'Radius': '{number}',
})

#request data from the WMATA API
try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jStops?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

#Convert data from json to dictionary to pd dataframe.
jsonData = json.loads(data)
pdData = pd.DataFrame.from_dict(jsonData['Stops'])

busStops = pdData.drop_duplicates(subset = ['StopID'])
busStops = busStops.set_index(['StopID'])

#Create and add bus stops data to Bus_Stops Table

engine = create_engine(f'postgresql://{config["postgreSQL"]["user"]}:{config["postgreSQL"]["password"]}@{config["postgreSQL"]["host"]}:{config["postgreSQL"]["port"]}/{config["postgreSQL"]["database"]}')

busStops.to_sql('Bus_Stops', 
                 con = engine,
                 if_exists = 'replace', #Future version should create a test to append this data rather than replace it.
                 index = True)