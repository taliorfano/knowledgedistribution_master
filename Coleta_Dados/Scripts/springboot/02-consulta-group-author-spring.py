import requests
import json
import csv
from dateutil import parser
from pymongo import MongoClient
from random import randint
from pprint import pprint
import dateutil
import datetime

#Conecta ao MongoDB
client = MongoClient('mongodb+srv://admin:admin123@cluster0-cdbu9.mongodb.net/test?retryWrites=true&w=majority')
db = client.contributions
date1 =  datetime.datetime(2019,1,1,0,0)

resultado = db.spring.aggregate([
    {"$match" : {
                "date": { 
                    "$lt": date1
                }
            }
        },
		{"$group" : {"_id":{"email":"$author.email", "name":"$author.name"}, "count":{"$sum":1}}}
	])

with open('coletas-spring-authors-group-sem 2019.csv', 'w', newline='', encoding='utf-8') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=';')
    spamwriter.writerow(['Name', 'E-mail', 'Total'])

    for resul in resultado:
        print(resul['_id']['name'], '-', resul['_id']['email'], '-', 'Total:', resul['count'])
        spamwriter.writerow([resul['_id']['name'], resul['_id']['email'],resul['count']])
