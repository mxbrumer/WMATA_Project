from ENV_Setup import *
from Bus_Position_Functions import *
from Postgres_Functions import *

#Create an API key variable.
APIKey = config['WMATA']['API_Key']


# Create bus position table and save to postgres################################################################################

busPositionsJson = bus_position_pull(APIKey, 
                                     RouteID = '',
                                     Lat = '',
                                     Lon = '',
                                     Radius = '')

busPsotitionsPd = convertbus_positions_json_to_pandas(busPositionsJson)

save_table_to_postgres(busPsotitionsPd, 'buspositions', schema = 'bronze', if_exists = 'replace')


# Create ongoing data pull####################################################################################################
while check_time(time(5,0), time(23,59)):
    busPositionsJson = bus_position_pull(APIKey, 
                                         RouteID = '',
                                         Lat = '',
                                         Lon = '',
                                         Radius = '')

    busPsotitionsPd = convertbus_positions_json_to_pandas(busPositionsJson)

    save_table_to_postgres(busPsotitionsPd, 'buspositions', schema = 'bronze', if_exists = 'append')

    time.sleep(10)