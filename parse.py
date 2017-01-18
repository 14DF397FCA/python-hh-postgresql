# -*- coding: utf-8 -*-
import requests
import psycopg2
import json
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

dbname = 'hh'
dbuser = 'python'
dbpass = 'qqqqqQ!1'
VAC_IDs_ACTUAL = {}
VAC_IDs_DB = {}
VAC_IDs_OLD = {}
VACANCIES_REMOVED = {}
PAGE = 0
vID = 0

VACANCIES_URL = 'https://api.hh.ru/vacancies?clusters=false&specialization=1&area=2&only_with_salary=true&per_page=100&page='
VACANCY_URL = "https://api.hh.ru/vacancies/"

RESP = requests.get(VACANCIES_URL + str(0))
DATA_LOADS = json.loads(RESP.text)

try:
    connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
    cursor = connect.cursor()
    cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", ('vacancies',))
    if cursor.fetchone()[0] == False:
        try:
            qCreateTable = "CREATE TABLE hh.vacancies (" \
                           "vac_id          INTEGER     UNIQUE, " \
                           "vac_name        VARCHAR(150) NOT NULL, " \
                           "alternate_url   VARCHAR(50) NOT NULL, " \
                           "premium         BOOLEAN     NOT NULL, " \
                           "description     TEXT        NOT NULL, " \
                           "schedule_id     VARCHAR(15) NOT NULL, " \
                           "schedule_name   VARCHAR(20) NOT NULL, " \
                           "experience_id   VARCHAR(15) NOT NULL, " \
                           "experience_name VARCHAR(20) NOT NULL, " \
                           "address         JSON, " \
                           "skills          TEXT, " \
                           "employment_id   VARCHAR(10) NOT NULL, " \
                           "employment_name VARCHAR(25) NOT NULL, " \
                           "salary          JSON, " \
                           "area_id         INTEGER, " \
                           "area_name       VARCHAR(30), " \
                           "employer        JSON, " \
                           "specializations JSON," \
                           "active          BOOLEAN     NOT NULL" \
                           ");"
            cursor.execute(qCreateTable)
            connect.commit()
        except psycopg2.DataError as exception:
            print(exception)
except psycopg2.DataError as exception:
    print(exception)


def markvacancyremoved(vacancy_id):
    try:
        connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
        cursor = connect.cursor()
        qUpdate = 'UPDATE hh.vacancies SET active = False WHERE vac_id = %s' % int(vacancy_id)
        cursor.execute(qUpdate)
        connect.commit()
        cursor.close()
    except psycopg2.DataError as exception:
        print(exception)
    finally:
        if connect:
            connect.close()


while PAGE < DATA_LOADS['pages']:
    RESP = requests.get(VACANCIES_URL + str(PAGE))
    DATA_LOADS = json.loads(RESP.text)
    for ITEM in DATA_LOADS['items']:
        VAC_DUMPS = json.dumps(ITEM)
        VAC_LOADS = json.loads(VAC_DUMPS)
        av = VAC_LOADS['id']
        VAC_IDs_ACTUAL[vID] = int(av)
        vID += 1
    PAGE += 1

vID = 0
try:
    connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
    cursor = connect.cursor()
    cursor.execute('SELECT vac_id FROM hh.vacancies')
    result = cursor.fetchall()
    for row in result:
        VAC_IDs_DB[vID] = int(row[0])
        vID += 1
    cursor.close()
except psycopg2.DataError as exception:
    print(exception)
finally:
    if connect:
        connect.close()

vID = 0

# for vID in range(len(VAC_IDs_DB)):
    # print(str(len(VAC_IDs_ACTUAL)))
    # print(str(vID))
while vID < len(VAC_IDs_DB):
    if VAC_IDs_DB[vID] in VAC_IDs_ACTUAL.values():
        #print("Vacancy with vID (%s) is active and already exist in database, remove it from the list" % (VAC_IDs_DB[vID]))
        #VAC_IDs_ACTUAL.pop(vID)
        #print(VAC_IDs_ACTUAL[vID]
        print(vID)
        # del VAC_IDs_ACTUAL[vID]
        # VAC_IDs_ACTUAL[vID] = 0
        print(VAC_IDs_ACTUAL[vID])
        print("-------------")
        #print(VAC_IDs_ACTUAL
        # quit()
    else:
        #print("Vacancy with vID (%s) does not available in the list, mark as removed in DB" % (VAC_IDs_DB[vID]))
        # VAC_IDs_OLD[len(VAC_IDs_OLD) + 1] = VAC_IDs_DB[vID]
        markvacancyremoved(VAC_IDs_DB[vID])
    #print(VACANCY_URL + str(VAC_IDs_DB[vID]))
    #print("----------------")
    vID += 1
# print(VAC_IDs_ACTUAL)
quit()
# print(VAC_IDs_ACTUAL)
# print(VAC_IDs_DB)
# print(VAC_IDs_OLD)

def importdataindb():
    vID = 0
    try:
        connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
        cursor = connect.cursor()
        #print(VAC_IDs_ACTUAL)
        for row in VAC_IDs_ACTUAL:
            #print(VAC_IDs_ACTUAL[row])
            RESP = requests.get(VACANCY_URL + str(VAC_IDs_ACTUAL[row]))
            VAC_BASE = json.loads(RESP.text)
            qIsExists = 'SELECT EXISTS(SELECT * FROM hh.vacancies WHERE vac_id = %s);' % int(VAC_BASE['id'])
            cursor.execute(qIsExists)
            if cursor.fetchone()[0] == False:
                VACANCY_KEY_SKILLS = ""
                VACANCY_COUNT = len(VAC_BASE['key_skills'])
                if VACANCY_COUNT > 0:
                    for KEY_SKILL in range(VACANCY_COUNT):
                        VACANCY_KEY_SKILLS += VAC_BASE['key_skills'][KEY_SKILL]['name'] + ";"
                qAddVacancy = "INSERT INTO hh.vacancies VALUES (" \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "\'%s\', " \
                              "True" \
                              ");" % (
                    int(VAC_BASE['id']),
                    VAC_BASE['name'],
                    VAC_BASE['alternate_url'],
                    VAC_BASE['premium'],
                    VAC_BASE['description'],
                    VAC_BASE['schedule']['id'],
                    VAC_BASE['schedule']['name'],
                    VAC_BASE['experience']['id'],
                    VAC_BASE['experience']['name'],
                    json.dumps(VAC_BASE['address'], ensure_ascii=False),
                    VACANCY_KEY_SKILLS,
                    VAC_BASE['employment']['id'],
                    VAC_BASE['employment']['name'],
                    json.dumps(VAC_BASE['salary'], ensure_ascii=False),
                    VAC_BASE['area']['id'],
                    VAC_BASE['area']['name'],
                    json.dumps(VAC_BASE['employer'], ensure_ascii=False),
                    json.dumps(VAC_BASE['specializations'], ensure_ascii=False)
                )
                print(vID, qAddVacancy)
                cursor.execute(qAddVacancy)
                connect.commit()
            vID += 1
        cursor.close()
    except psycopg2.DataError as exception:
        print(exception)
    finally:
        if connect:
            connect.close()

importdataindb()
