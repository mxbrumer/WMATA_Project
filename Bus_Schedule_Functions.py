from ENV_Setup import *

# Bronze Level Functions ###################################################################################################################

# Pull the raw data from the WMATA API. 
# Must specify APIKey and routeId
# Optional specify a date for the schedule in YYYY-MM-DD format. Default is today's date. 
# Optional specify the inclusion of bus schedule variations. Default is False.
# Returns data in a raw json format. 
def pull_bus_schedule_from_api(APIKey, routeId, date = '', variations = False):
    #Set API Key
    headers = {
        # Request headers 
        'api_key': f'{APIKey}',
        }
    
    #Set requested parameters    
    params = urllib.parse.urlencode({
        # Request parameters
        'RouteID': f'{routeId}',
        'Date': date,
        'IncludingVariations': variations
        })
    
    #Retrieve data
    try:
        conn = http.client.HTTPSConnection('api.wmata.com')
        conn.request("GET", "/Bus.svc/json/jRouteSchedule?%s" % params, "{body}", headers)
        response = conn.getresponse()
        jsonData = response.read()
        conn.close()
    except Exception as e:
        print("[Errno {0}] {1}".format(e.errno, e.strerror))

    return jsonData



# Convert bus schedule from jsonData to a pandas dictionary. 
# Must specify jsonData
# Returns pandas dataframe for directions 0 and 1. 
def convert_json_data_to_pandas(jsonData):
    if jsonData == b'{"Message":"Pattern/route name not specified, invalid, or does not exist in this schedule."}':
        busRoute0 = []
        busRoute1 = []
        return busRoute0, busRoute1  
    else:
        #Convert data to dict format
        jsonData = json.loads(jsonData)
        #Convert data to pandas dataframe for the first direction. Most routes (but not all) have data within the Direction0 dictionary.
        busRoute0 = pd.DataFrame.from_dict(jsonData['Direction0'])
        #Convert data to pandas dataframe for the second direction. If the bus only moves in one direction (i.e. a loop) then this will be null
        busRoute1 = pd.DataFrame.from_dict(jsonData['Direction1'])
        return busRoute0, busRoute1


# Silver Level Functions ###################################################################################################################
def convert_series_of_strings_to_series_of_dict(series):
    seriesOfDicts = series.apply(lambda row: ast.literal_eval(row))
    return seriesOfDicts

def expand_series_of_dicts_to_pandas(seriesOfDicts):
    expandedPd = pd.DataFrame()
    for row in range(0,len(seriesOfDicts)):
        expandedPd = pd.concat([expandedPd, pd.DataFrame.from_dict(seriesOfDicts[row])])
    return expandedPd



# Seperate data by direction
def split_route_by_bus_direction(routePd):
    routePd0 = routePd[routePd['DirectionNum'] == '0']
    routePd1 = routePd[routePd['DirectionNum'] == '1'] 

    return routePd0, routePd1



def combine_pd_dataframes_for_both_directions(routePd0, routePd1, stopTimesExpandedPd0, stopTimesExpandedPd1):
    if pd.isnull(routePd0).all().all() and pd.isnull(routePd1).all().all():
        return

    if pd.isnull(routePd0).all().all() == False and pd.isnull(routePd1).all().all() == True:
        stopTimesExpandedPdFull = stopTimesExpandedPd0
        return stopTimesExpandedPdFull

    if pd.isnull(routePd0).all().all() == True and pd.isnull(routePd1).all().all() == False:
        stopTimesExpandedPdFull = stopTimesExpandedPd1
        return stopTimesExpandedPdFull

    if pd.isnull(routePd0).all().all() == False and pd.isnull(routePd1).all().all() == False:
        stopTimesExpandedPdFull = pd.concat([stopTimesExpandedPd0, stopTimesExpandedPd1])
        return stopTimesExpandedPdFull

# Gold Level Functions #####################################################################################################################
