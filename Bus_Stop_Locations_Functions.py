from ENV_Setup import *

# Bronze Level Functions ###################################################################################################################

# Pull the raw data from the WMATA API. 
# Must specify APIKey and routeId
# Optional specify a date for the schedule in YYYY-MM-DD format. Default is today's date. 
# Optional specify the inclusion of bus schedule variations. Default is False.
# Returns data in a raw json format. 
def pull_bus_stop_locations_from_api(APIKey, latitude = '', longitude = '', radius = ''):
    headers = {
        # Request headers
        'api_key': APIKey
    }

    #Get requested parameters
    params = urllib.parse.urlencode({
        # Request parameters. Left blank to capture all bus stop locations
        'Lat': f'{latitude}',
        'Lon': f'{longitude}',
        'Radius': f'{radius}',
    })

    #request data from the WMATA API
    try:
        conn = http.client.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jStops?%s" % params, "{body}", headers)
        response = conn.getresponse()
        jsonData = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))

    return jsonData

def convert_stop_location_json_to_pandas(jsonData):
    jsonData = json.loads(jsonData)
    pdData = pd.DataFrame.from_dict(jsonData['Stops'])

    #busStops = pdData.drop_duplicates(subset = ['StopID'])
    busStopsPd = pdData.set_index(['StopID'])

    return busStopsPd



# Silver Level Functions ###################################################################################################################

# Gold Level Functions #####################################################################################################################