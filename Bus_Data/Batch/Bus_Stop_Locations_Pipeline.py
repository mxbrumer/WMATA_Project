from ENV_Setup import *
from Bus_Stop_Locations_Functions import *
from Postgres_Functions import *

# Create bronze level route tables and save to postgres################################################################################
APIKey = config['WMATA']['api_key']
table = 'busstoplocations'
schema = 'bronze'

locationsJson = pull_bus_stop_locations_from_api(APIKey, latitude = '', longitude = '', radius = '')

locationsPd = convert_stop_location_json_to_pandas(locationsJson)

save_table_to_postgres(locationsPd, table, schema)

