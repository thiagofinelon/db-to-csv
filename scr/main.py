import ast
import csv
import decimal
import json as json
import pprint as pp
import sys
from math import *

import boto3
from boto3.dynamodb.conditions import Attr, Key


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def mergeDict(dict1, dict2):
    dict3 = {**dict1, **dict2}
    for key, value in dict3.items():
        if key in dict1 and key in dict2:
            dict3[key] = [value , dict1[key]]
            return dict3
def toDict(a): 
    it = iter(a) 
    res_dct = dict(zip(it, it)) 
    return res_dct 

def main():
    argc = len(sys.argv)
    if argc != 3:
        print('Inavlid input!')
        return 

    if int(sys.argv[1]) == 1:
        client = boto3.client('dynamodb')
        response_from_list = client.list_tables()
        
        for ele in enumerate(response_from_list['TableNames']): 
            print (ele) 

        while True:
            table_select = eval(input('Wich table: '))
            if table_select > len(response_from_list['TableNames']) - 1:
                print('Invalid option, try again!')
            else:
                break

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
    
        csv_table = csv.writer(open(str(response_from_list['TableNames'][int(table_select)]) + '-' + str(primary_key) + '.csv', '+w', newline=''),  delimiter=';', quotechar='"', quoting=csv.QUOTE_NONE, escapechar='\\')
        #csv_table = csv.writer(open(str(response_from_list['TableNames'][int(table_select)]) + '-' + str(primary_key) + '.csv', '+w', newline=''))
        csv_table.writerow(response['Items'][0].keys())
        if sys.argv[2] == 1:
            print('Exporting minute by minute')
            for row in json_load:
                csv_table.writerow(list(row.values()))
        else:
            print('Exporting '+ str(sys.argv[2]) +' minute by '+ str(sys.argv[2]) +' minute')
            for row in json_load:
                csv_table.writerow(list(row.values()))
        
        print('CSV created with success!')
   
   
    elif int(sys.argv[1]) == 2:
        client = boto3.client('dynamodb')
        response_from_list = client.list_tables()
    
        for ele in enumerate(response_from_list['TableNames']): 
            print (ele) 
        
        invalid_input = True
        while invalid_input:
            input_list = eval(str(input('Wich tables(Exemple: 1 2 3 ...): ').split(' ')))
            invalid_input = False
            for i in input_list:
                if int(i) >  len(response_from_list['TableNames']) - 1:
                    invalid_input = True
                    print('Invalid input!')

        dynamodb = boto3.resource('dynamodb')

        number_of_tables =  int(len(input_list))
        
        table_names = number_of_tables * [0]
        tables = number_of_tables * [0]
        describe_list = number_of_tables * [0]
        primary_key_list = number_of_tables * [0]
        response_list = number_of_tables * [0]
        query_len_list = number_of_tables * [0]

        for i in range(number_of_tables):
            table_names[i] = response_from_list['TableNames'][int(input_list[i])]
            tables[i] = dynamodb.Table(table_names[i])
            describe_list[i] = client.describe_table(
                TableName = table_names[i]
            )
            primary_key_list[i] = eval(input('Set the primary key from "'+ table_names[i] +'" for search(' + describe_list[i]['Table']['AttributeDefinitions'][0]['AttributeName'] + '): '))
            response_list[i] = tables[i].query(
                KeyConditionExpression=Key(str(describe_list[i]['Table']['AttributeDefinitions'][0]['AttributeName'])).eq(int(primary_key_list[i]))
            )
            query_len_list[i] = len(response_list[i]['Items'])    


        response = max(query_len_list) * []
        aux = {}
        for i in range(0, (number_of_tables - 1), 2):
            l = 0
            while (l < (query_len_list[i] - 1)) & (l < (query_len_list[i + 1] - 1)):
                aux = {**response_list[i]['Items'][l], **response_list[i + 1]['Items'][l]}
                response.append(aux.copy())
                l += 1

            while (l < (query_len_list[i] - 1)):
                aux = {**response_list[i]['Items'][l]}
                response.append(aux.copy())
                l += 1

            while (l < (query_len_list[i + 1] - 1)):
                aux = {**response_list[i + 1]['Items'][l]}
                response.append(aux.copy())
                l += 1

        name_file = ''
        for i in range(number_of_tables):
            name_file = name_file + '-' + str(table_names[i]) 
            
        csv_table = csv.writer(open('merged' + str(name_file) + str(primary_key_list[0]) +'-'+ '.csv', '+w', newline=''),  delimiter=';', quotechar='"', quoting=csv.QUOTE_NONE, escapechar='\\')
        csv_table.writerow(response[0].keys())
        if sys.argv[2] == 1:
            print('Exporting minute by minute')
            for row in response:
                csv_table.writerow(list(row.values()))
        else:
            print('Exporting '+ str(sys.argv[2]) +' minute by '+ str(sys.argv[2]) +' minute')
            for row in response:
                csv_table.writerow(list(row.values()))
        
        print('CSV created with success!')
            
    else:
        print('Invalid option!')
       

if __name__ == '__main__':
  main()
