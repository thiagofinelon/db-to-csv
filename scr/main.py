import csv
import boto3
import json as json
import decimal
import pprint as pp
import ast
from boto3.dynamodb.conditions import Key, Attr

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def main():
    print('1. Single table')
    print('2. Multiple tables')
    input_select = eval(input('Select the type of export: '))
    if int(input_select) == 1:
        single_table()
    elif int(input_select) == 2:
        mult_tables()
    else:
        print('Invalid option!')


def single_table():
    client = boto3.client('dynamodb')
    response_from_list = client.list_tables()
    
    for ele in enumerate(response_from_list['TableNames']): 
        print (ele) 

    table_select = eval(input('Wich table: '))
    table_name = str(response_from_list['TableNames'][int(table_select)])

    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table(table_name)
    
    describe = client.describe_table(
        TableName = table_name
    )

    primary_key = eval(input('Set the primary key for search(' + describe['Table']['AttributeDefinitions'][0]['AttributeName'] + '): '))
    
    print('Querying from table "' + str(response_from_list['TableNames'][int(table_select)]) + '" ...')
    response = table.query(
        KeyConditionExpression=Key(str(describe['Table']['AttributeDefinitions'][0]['AttributeName'])).eq(int(primary_key))
    )
    
    
    query_len = len(response['Items'])
    print('Query has fineshed with ' + str(query_len) + ' items!')

    json_load = query_len * [0]

    for i in range(0, query_len):   
        json_load[i] = ast.literal_eval((json.dumps(response['Items'][i], cls=DecimalEncoder)))


 
#    csv_table = csv.writer(open(str(response_from_list['TableNames'][int(table_select)]) + '-' + str(primary_key) + '.csv', '+w', newline=''),  delimiter=';', quotechar='"', quoting=csv.QUOTE_NONE, escapechar='\\')
    csv_table = csv.writer(open(str(response_from_list['TableNames'][int(table_select)]) + '-' + str(primary_key) + '.csv', '+w', newline=''))

    csv_table.writerow(response['Items'][0].keys())

    for row in json_load:
        csv_table.writerow(list(row.values()))
    
    print('CSV created with success!')
   

def mult_tables():
    client = boto3.client('dynamodb')
    response_from_list = client.list_tables()
    
    for ele in enumerate(response_from_list['TableNames']): 
        print (ele) 


main()