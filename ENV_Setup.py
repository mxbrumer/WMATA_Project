#Setup variables thazt multiple files will need.

#Import general packages
import pandas as pd
import json
import http.client, urllib.request, urllib.parse, urllib.error, base64
import psycopg2
from datetime import datetime, time
import ast


#Import config file
import configparser
config = configparser.ConfigParser()
config.read('config.ini')

#Connect to postgreSql
from sqlalchemy import create_engine
engine = create_engine(f'postgresql://{config["postgreSQL"]["user"]}:{config["postgreSQL"]["password"]}@{config["postgreSQL"]["host"]}:{config["postgreSQL"]["port"]}/{config["postgreSQL"]["database"]}')

