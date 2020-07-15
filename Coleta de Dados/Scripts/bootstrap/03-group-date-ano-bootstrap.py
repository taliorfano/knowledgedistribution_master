import requests
import json
import csv
import datetime
from dateutil import parser
from pymongo import MongoClient
from random import randint
from pprint import pprint
import dateutil

#Conecta ao MongoDB
client = MongoClient('mongodb+srv://admin:admin123@cluster0-cdbu9.mongodb.net/test?retryWrites=true&w=majority')
db = client.contributions

date1 =  datetime.datetime(2011,1,1,0,0)
date2 = date1
date2 = date2.replace(year=date1.year + 1)
num = 1

while date1 < datetime.datetime.now():
    print(date1)
    print(date2)
    resultado = db.bootstrap.aggregate([
        {"$match" : {
            "date": { 
                "$gte": date1,
                "$lt": date2
            }
        }
        },
        {"$group" : {
            "_id":{"email":"$author.email", "name":"$author.name"}, 
            "count":{"$sum":1}}
        }
    ])

    name = 'coletas-bootstrap-authors-ano'+str(num)+'.csv'
    print(name)

    with open(name, 'w', newline='', encoding='utf-8') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=';')
        spamwriter.writerow(['Name', 'E-mail', 'Total'])
        
        for group in resultado:
            spamwriter.writerow([group['_id']['name'], group['_id']['email'],group['count']])
        
    date1 = date2
    date2 = date2.replace(year=date1.year + 1)
    num = num + 1






