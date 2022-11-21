
# скрипт скачивает данные из API, сохраняет данные в файлы .json и .csv в папку проекта и загружает в таблицу в базе SQLLite

from urllib import request
import json
import pandas as pd
import datetime
import sqlite3

N_Users = 500
URL = 'https://randomuser.me/api/1.3/?results={}&inc=gender,name,location,email,login,phone'.format(N_Users)
Root_Path = '' #сохраняю файлы прямо в проект

# берем данные из API и кладем их в .json файл
randomUsersJson = json.loads(request.urlopen(URL).read())

with open(Root_Path + 'RandomUsers.json', 'w') as f:
    json.dump(randomUsersJson, f)

print("first 5 string in .json file: \n")
for itm in randomUsersJson['results'][:5]:
    print(itm)

# через DataFrame пишем в CSV
data = []
dateLoaded = datetime.datetime.now().timestamp()
for itm in randomUsersJson['results']:
    data.append((
                itm['gender'],
                itm['name']['first'] + ' ' + itm['name']['last'],
                itm['name']['first'],
                itm['name']['last'],
                itm['location']['city'],
                itm['email'],
                itm['login']['md5'],
                itm['phone'],
                dateLoaded
    ))

df = pd.DataFrame(data, columns=['Gender', 'Name', 'First name', 'Last name', 'City', 'Email', 'Md5 login', 'Phone number', 'Date loaded'])
df.to_csv(Root_Path + 'randomusers.csv', index=False)

pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 2500)

print("\n first 5 string in .csv file: \n")
print(pd.read_csv(Root_Path + 'randomusers.csv', nrows=5))

# создаем базу в SQLLite с таблицей RandomUsers
conn = sqlite3.connect('DB_Users.db')

cmdCreateUsers = '''CREATE TABLE RandomUsers (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT,
                            Gender TEXT NULL,
                            Name TEXT NULL,
                            First_Name TEXT NULL,
                            Last_Name TEXT NULL,
                            City TEXT NULL,
                            Email TEXT NULL,
                            Md5_Login TEXT NULL,
                            Phone_Number TEXT NULL,
                            Date_Loaded datetime NULL
                            );'''

cursor = conn.cursor()

try:
    cursor.execute("Drop table RandomUsers")
    cursor.execute(cmdCreateUsers)
    conn.commit()
except sqlite3.Error as error:
    pass
finally:
    pass

cursor.executemany("INSERT INTO RandomUsers VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?);", data)
conn.commit()

print("\n first 5 string in Database table: \n")
print(pd.DataFrame(cursor.execute("SELECT * FROM RandomUsers;").fetchmany(5),
                    columns=['Id', 'Gender', 'Name', 'First name', 'Last name', 'City', 'Email', 'Md5 login', 'Phone number', 'Date loaded'])
                   )
