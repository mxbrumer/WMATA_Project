from ENV_Setup import *
from Bus_Position_Functions import *

#Create an API key variable.
APIKey = config['WMATA']['API_Key']

if check_time(time(0,0), time(0,1)) == True:
    #ADD FUNCTION FOR ARCHIVING OLD DATA
    bus_position_delete('Bus_Positions')

else:
    #Pull bus position data
    busPositions = bus_position_pull(APIKey, 
                                     RouteID = '',
                                     Lat = '',
                                     Lon = '',
                                     Radius = '')
    
    #Create table if this is the first pull of the day
    if check_time(time(5,0), time(5,1)) == True:
        bus_position_create(busPositions)

    #Append the table for each subsequent data pull
    else:
        bus_position_append(busPositions)
