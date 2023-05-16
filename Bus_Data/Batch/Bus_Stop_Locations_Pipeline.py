from ENV_Setup import *
from Bus_Stop_Locations_Functions import *
from Postgres_Functions import *
from Bus_Schedule_Functions import *

APIKey = config['WMATA']['api_key']
table = 'busstoplocations'

# Create bronze level route tables and save to postgres################################################################################

locationsJson = pull_bus_stop_locations_from_api(APIKey, latitude = '', longitude = '', radius = '')

locationsPd = convert_stop_location_json_to_pandas(locationsJson)

save_table_to_postgres(locationsPd, table, schema = 'bronze')

# Create gold level route tables and save to postgres################################################################################

locationsPd = retireve_sql_table(schema = "bronze",
                                 table = table)

locationsPd = convert_text_to_int(locationsPd, 'StopID')

save_table_to_postgres(locationsPd, table, schema = 'gold')
