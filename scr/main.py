import csv
import boto3
import json as json
import decimal
import pprint
import ast
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


day_mon_year = eval(input('Set a timestamp (yyyymmdd): '))


dynamodb = boto3.resource('dynamodb')
table    = dynamodb.Table('ambientais_ufms')


print('Querying from table "ambientais_ufms"...')
response = table.query(
    KeyConditionExpression=Key('dia_mes_ano').eq(day_mon_year)
)
query_len = len(response['Items'])
print('Query has fineshed with ' + str(query_len) + ' items!')

json_load = query_len * [0]
for i in range(0, query_len):
    json_load[i] = ast.literal_eval((json.dumps(response['Items'][i], cls=DecimalEncoder)))

csv_table = csv.writer(open('station_' + str(day_mon_year) + '.csv', '+w'))
csv_table.writerow(["dia_mes_ano","hora_minuto","rainfall","irr","temp","vento_dir","vento_vel","massaPM1","massaPM2","massaPM4","massaPM10","numPM1","numPM2","numPM4","numPM10","tamanho_medio"])
for json_load in json_load:
    if((json_load.hora_minuto / 10)  % 15 == 0)
        csv_table.writerow( [json_load["dia_mes_ano"], 
                        json_load["hora_minuto"], 
                        json_load["rainfall"], 
                        json_load["irr"], 
                        json_load["temp"], 
                        json_load["vento_dir"], 
                        json_load["vento_vel"], 
                        json_load["massaPM1"], 
                        json_load["massaPM2"], 
                        json_load["massaPM4"], 
                        json_load["massaPM10"], 
                        json_load["numPM1"], 
                        json_load["numPM2"], 
                        json_load["numPM4"], 
                        json_load["numPM10"], 
                        json_load["tamanho_medio"]])
