# -*- coding: utf-8 -*-
from ctypes.wintypes import RECT

import requests
import psycopg2
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

dbname = 'hh'
dbuser = 'python'
dbpass = 'qqqqqQ!1'
# VACANCIES = {}
VAC_IDs_ACTUAL = {}
VAC_IDs_DB = {}
VAC_IDs_OLD = {}
VAC_IDs_NEW = {}
VACANCIES_REMOVED = {}
PAGE = 0
vID = 0

VACANCIES_URL = 'https://api.hh.ru/vacancies?clusters=false&specialization=1&area=2&only_with_salary=true&per_page=100&page='
VACANCY_URL = "https://api.hh.ru/vacancies/"

RESP = requests.get(VACANCIES_URL + str(0))
DATA_LOADS = json.loads(RESP.text)

# Create table hh.vacancies

def markvacancyremoved(vacancy_id):
    try:
        connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
        cursor = connect.cursor()
        qUpdate = 'UPDATE hh.vacancies SET active = False WHERE vac_id = %s' % vacancy_id
        #print qUpdate
        cursor.execute(qUpdate)
        connect.commit()
        cursor.close()
    except psycopg2.DataError, exception:
        print exception
    finally:
        if connect:
            connect.close()


while PAGE < DATA_LOADS['pages']:
# while PAGE < 3:
    RESP = requests.get(VACANCIES_URL + str(PAGE))
    DATA_LOADS = json.loads(RESP.text)
    for ITEM in DATA_LOADS['items']:
        VAC_DUMPS = json.dumps(ITEM)
        VAC_LOADS = json.loads(VAC_DUMPS)
        av = VAC_LOADS['id']
        VAC_IDs_ACTUAL[vID] = str(av)
        #VACANCIES[ITEM['id']] = ITEM
        vID += 1
    PAGE += 1
#print VAC_IDs_ACTUAL

vID = 0
try:
    connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
    cursor = connect.cursor()

    cursor.execute('SELECT vac_id FROM hh.vacancies')
    a = cursor.fetchall()
    vID = 0
    for i in a:
        #print i[0]
        VAC_IDs_DB[vID] = str(i[0])
        vID += 1
    cursor.close()
except psycopg2.DataError, exception:
    print exception
finally:
    if connect:
        connect.close()

# print VAC_IDs_ACTUAL
# print VAC_IDs_DB

vID = 0
for vID in range(len(VAC_IDs_DB)):
    if VAC_IDs_DB[vID] in VAC_IDs_ACTUAL.values():
        #print "Vacancy with vID (%s) is active" % (VAC_IDs_DB[vID])
        #VAC_IDs_ACTUAL.pop(vID)
        #print VAC_IDs_ACTUAL[vID]
        del VAC_IDs_ACTUAL[vID]
        #print VAC_IDs_ACTUAL
        # quit()
    else:
        # print "Vacancy with vID (%s) was removed" % (VAC_IDs_DB[vID])
        markvacancyremoved(VAC_IDs_DB[vID])
    vID += 1

vID = 0
#print VAC_IDs_ACTUAL
try:
    connect = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
    cursor = connect.cursor()
    for row in VAC_IDs_ACTUAL:
        # print str(VACANCY_URL + str(VAC_IDs_ACTUAL[row]))
        RESP = requests.get(VACANCY_URL + str(VAC_IDs_ACTUAL[row]))
        VAC_BASE = json.loads(RESP.text)
        qIsExists = 'SELECT EXISTS(SELECT * FROM hh.vacancies WHERE vac_id = %s)' % (VAC_BASE['id'].encode('utf-8'))
        print qIsExists
        cursor.execute(qIsExists)
        if cursor.fetchone()[0] == False:
            VACANCY_KEY_SKILLS = ""
            VACANCY_COUNT = len(VAC_BASE['key_skills'])
            if VACANCY_COUNT > 0:
                for KEY_SKILL in range(VACANCY_COUNT):
                    VACANCY_KEY_SKILLS += VAC_BASE['key_skills'][KEY_SKILL]['name'].encode('utf-8') + ";".encode(
                        'utf-8')
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
                VAC_BASE['id'].encode('utf-8'),
                VAC_BASE['name'].encode('utf-8'),
                VAC_BASE['alternate_url'].encode('utf-8'),
                VAC_BASE['premium'],
                VAC_BASE['description'].encode('utf-8'),
                VAC_BASE['schedule']['id'].encode('utf-8'),
                VAC_BASE['schedule']['name'].encode('utf-8'),
                VAC_BASE['experience']['id'].encode('utf-8'),
                VAC_BASE['experience']['name'].encode('utf-8'),
                json.dumps(VAC_BASE['address'], ensure_ascii=False),
                VACANCY_KEY_SKILLS,
                VAC_BASE['employment']['id'].encode('utf-8'),
                VAC_BASE['employment']['name'].encode('utf-8'),
                json.dumps(VAC_BASE['salary'], ensure_ascii=False),
                VAC_BASE['area']['id'].encode('utf-8'),
                VAC_BASE['area']['name'].encode('utf-8'),
                json.dumps(VAC_BASE['employer'], ensure_ascii=False),
                json.dumps(VAC_BASE['specializations'], ensure_ascii=False)
            )
            print vID, qAddVacancy
            cursor.execute(qAddVacancy)
            connect.commit()
        vID += 1
    cursor.close()
except psycopg2.DataError, exception:
    print exception
finally:
    if connect:
        connect.close()
