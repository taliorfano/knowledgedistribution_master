import requests
import json
import csv
from dateutil import parser
from pymongo import MongoClient
from random import randint
from pprint import pprint

# Dados de acesso ao GitHub
username = 'XXXXXXXX'
token = 'XXXXXXXX'

# Linguagem escolhida
languages = ['Java']
totalLanguages = len(languages)

# Criterios de prioridade -> repositorios que possuem mais 'stars'
sort = 'stars'
order = 'desc'

#Conecta ao MongoDB
client = MongoClient('mongodb+srv://admin:admin123@cluster0-cdbu9.mongodb.net/test?retryWrites=true')
db = client.contributions

# Obtem dados da linguagem analisada e seus respectivos projetos, na ordem de relevancia
for l in range(totalLanguages): 

    language = languages[l]
    print(language, ':', sep='')
    #repos = requests.get('https://api.github.com/search/repositories?q=language:' + language + '&sort=' + sort + '&order=' + order + '&per_page=1')
    repos = requests.get('https://api.github.com/search/repositories?q=elasticsearch&language:' + language+'&sort=' + sort + '&order=' + order + '&per_page=1')
    reposItem = json.loads(repos.text or repos.content)
    
    # Quantidade de projetos obtidos
    reposSize = len(reposItem['items'])
    
    # Obtem dados de cada repositorio analisado
    for i in range(reposSize):
        
        # Principais dados dos projetos
        name = reposItem['items'][i]['full_name']
        
        print(name)

        createdAt = reposItem['items'][i]['created_at']
        contributorsUrl = reposItem['items'][i]['contributors_url']
        print(name, createdAt, contributorsUrl)
        
        commits_url_api = reposItem['items'][i]['commits_url']
        commits_url = commits_url_api.split('{')[0]
        
        print(commits_url)
        
        #TODO Calcular quantos contribuidores tem no repositorio e iterar o total de paginas: total_contribuidores/100
        page = 1 
        
        with open('coletas-elastic.csv', 'w', newline='', encoding='utf-8') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';')
            spamwriter.writerow(['Sha', 'Login', 'Name', 'E-mail', 'Date', 'Repository'])
            
            while(page<=480):
        
                commits = requests.get(commits_url, '?&per_page=100&page=' + str(page), auth=(username,token))
                commitsItem = json.loads(commits.text or commits.content)
                
                commitsSize = (len(commitsItem))

                print('------------------------------------')
                print('Page commits:', page)

                #TODO validar total de commits

                for c in range(commitsSize):
                    if commitsItem[c]['commit'] is None:
                        print(commitsItem[c], 'Nao tem')
                        continue
                    
                    commit = commitsItem[c]['commit']
                    c_sha = commitsItem[c]['sha']
                    
                    if commitsItem[c]['author'] is not None:
                        c_login = commitsItem[c]['author']['login']
                    
                    if commit['author'] is not None:
                        c_name = commit['author']['name']
                        c_email = commit['author']['email']
                        c_date = commit['author']['date']

                    c_name = str(c_name)
                    c_date_convert = parser.parse(c_date)
                    print(c_sha, c_login, c_name, c_email, c_date_convert, name)

                    contributions = {
                        'sha': c_sha,
                        'author': {'login': c_login, 'name' : c_name, 'email': c_email},
                        'date': c_date_convert
                    }

                    result = db.elasticsearch.insert_one(contributions)
                    spamwriter.writerow([c_sha, c_login, c_name, c_email, c_date_convert, name])
                    print('------------------------------------')
                page = page + 1
