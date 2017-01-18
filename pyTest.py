import json
import requests
import csv
import os.path
import tempfile
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

VACANCIES_URL = 'https://api.hh.ru/vacancies?clusters=false&specialization=1&area=2&only_with_salary=true&per_page=100&page='
VACANCY_URL = "https://api.hh.ru/vacancies/"
TEMPF = tempfile.gettempdir()
if TEMPF[1] == ':':
    CSVFILENAME = TEMPF + "\\" + "pyTest.csv"
else:
    CSVFILENAME = TEMPF + "/" + "pyTest.csv"
print(CSVFILENAME)
quit()

if os.path.exists(CSVFILENAME) == False:
    with open(CSVFILENAME, 'w') as outFile:
        outFile.write("")
        outFile.close()

RESP = requests.get(VACANCIES_URL + str(0))
DATA_LOADS = json.loads(RESP.text)
PAGECOUNT = DATA_LOADS['pages']
VACANCIES = {}
VACANCIES_REMOVED = {}
PAGE = 0
# while PAGE < PAGECOUNT:
while PAGE < DATA_LOADS['pages']:
#while PAGE < 3:
    RESP = requests.get(VACANCIES_URL + str(PAGE))
    DATA_LOADS = json.loads(RESP.text)
    for ITEM in DATA_LOADS['items']:
        VACANCIES[ITEM['id']] = ITEM
    PAGE += 1


def vacancyisremoved(vacancy_id, filename):
    inFile = open(filename, 'r').readlines()
    with open(filename, 'w') as outFile:
        for index, line in enumerate(inFile, 1):
            if vacancy_id in line:
                outFile.write(line.replace('Active', 'Removed'))
            else:
                outFile.write(line)
    outFile.close()
    return


rows = {}
#   Read already exist vacancies
print("Read already exist vacancies")
with open(CSVFILENAME, 'rb') as CSVFILE:
    VACANCIES_REMOVED_ID = 0
    rreader = csv.reader(CSVFILE, dialect='excel')
    for row in rreader:
        if row[0] in VACANCIES:
            del VACANCIES[row[0]]
        else:
            VACANCIES_REMOVED[VACANCIES_REMOVED_ID] = str(row[0])
            VACANCIES_REMOVED_ID += 1

# Add new vacancies
print("Add new vacancies")
with open(CSVFILENAME, 'a') as CSVFILE:
    wwriter = csv.writer(CSVFILE, dialect='excel')
    for i in VACANCIES:
        VACANCY = VACANCIES[i]
        VACANCY_DUMPS = json.dumps(VACANCY)
        VACANCY_LOADS = json.loads(VACANCY_DUMPS)
        VACANCY_RESP = requests.get(VACANCY_URL + str(VACANCY_LOADS['id']))
        VACANCY_DATA = json.loads(VACANCY_RESP.text)
        VACANCY_KEY_SKILLS = ""
        VACANCY_COUNT = len(VACANCY_DATA['key_skills'])
        if VACANCY_COUNT > 0:
            for KEY_SKILL in range(VACANCY_COUNT):
                VACANCY_KEY_SKILLS += VACANCY_DATA['key_skills'][KEY_SKILL]['name'].encode('utf-8') + ";".encode('utf-8')

        # VACANCY_COUNT = len(VACANCY_DATA['specializations'])
        # if VACANCY_COUNT > 0:
        # for PROFAREA in range(VACANCY_COUNT):
        # print(VACANCY_DATA['specializations'][PROFAREA]['profarea_id']
        # print(VACANCY_DATA['specializations'][PROFAREA]['profarea_name']
        # print(VACANCY_DATA['specializations'][PROFAREA]['id']
        # print(VACANCY_DATA['specializations'][PROFAREA]['name']

        if 'address''city' not in VACANCY_DATA:
            VACANCY_ADDRESS_CITY = "Null"
        else:
            VACANCY_ADDRESS_CITY = VACANCY_DATA['address']['city']

        if 'address''street' not in VACANCY_DATA:
            VACANCY_ADDRESS_STREET = "Null"
        else:
            VACANCY_ADDRESS_STREET = VACANCY_DATA['address']['street']

        if 'address''building' not in VACANCY_DATA:
            VACANCY_ADDRESS_BUILDING = "Null"
        else:
            VACANCY_ADDRESS_BUILDING = VACANCY_DATA['address']['building']

        if 'address''metro_stations''line_id' not in VACANCY_DATA:
            VACANCY_ADDRESS_METRO_LINE_ID = "Null"
            VACANCY_ADDRESS_METRO_LINE_NAME = "Null"
        else:
            VACANCY_ADDRESS_METRO_LINE_ID = VACANCY_DATA['address']['metro_stations']['line_id'].encode('utf-8'),
            VACANCY_ADDRESS_METRO_LINE_NAME = VACANCY_DATA['address']['metro_stations']['line_name'].encode('utf-8'),

        if 'address''metro_stations''station_id' not in VACANCY_DATA:
            VACANCY_ADDRESS_METRO_STATION_ID = "Null"
            VACANCY_ADDRESS_METRO_STATION_NAME = "Null"
        else:
            VACANCY_ADDRESS_METRO_STATION_ID = VACANCY_DATA['address']['metro_stations']['station_id'].encode('utf-8')
            VACANCY_ADDRESS_METRO_STATION_NAME = VACANCY_DATA['address']['metro_stations']['station_name'].encode('utf-8')
        if 'employer''id' not in VACANCY_DATA:
            VACANCY_EMPLOYER_ID = "Null"
            VACANCY_EMPLOYER_NAME = "Null"
        else:
            VACANCY_EMPLOYER_ID = VACANCY_DATA['employer']['id'].encode('utf-8')
            VACANCY_EMPLOYER_NAME = VACANCY_DATA['employer']['name'].encode('utf-8')

        print(VACANCY_URL + str(VACANCY_LOADS['id'].encode('utf-8')))
        wwriter.writerow([
            VACANCY_LOADS['id'].encode('utf-8'),
            VACANCY_LOADS['name'].encode('utf-8'),
            VACANCY_DATA['alternate_url'].encode('utf-8'),
            str(VACANCY_DATA['premium']),
            VACANCY_DATA['description'].encode('utf-8'),
            VACANCY_DATA['schedule']['id'].encode('utf-8'),
            VACANCY_DATA['schedule']['name'].encode('utf-8'),
            VACANCY_DATA['experience']['id'].encode('utf-8'),
            VACANCY_DATA['experience']['name'].encode('utf-8'),
            VACANCY_ADDRESS_CITY.encode('utf-8'),
            VACANCY_ADDRESS_STREET.encode('utf-8'),
            VACANCY_ADDRESS_BUILDING.encode('utf-8'),
            VACANCY_ADDRESS_METRO_LINE_ID,
            VACANCY_ADDRESS_METRO_LINE_NAME,
            VACANCY_ADDRESS_METRO_STATION_ID,
            VACANCY_ADDRESS_METRO_STATION_NAME,
            VACANCY_KEY_SKILLS.encode('utf-8'),
            VACANCY_DATA['employment']['id'].encode('utf-8'),
            VACANCY_DATA['employment']['name'].encode('utf-8'),
            VACANCY_DATA['salary']['to'],
            VACANCY_DATA['salary']['from'],
            VACANCY_DATA['salary']['currency'].encode('utf-8'),
            VACANCY_DATA['area']['id'].encode('utf-8'),
            VACANCY_DATA['area']['name'].encode('utf-8'),
            VACANCY_EMPLOYER_ID,
            VACANCY_EMPLOYER_NAME,
            "Active"
        ])

# Mark removed vacancies
print("Mark removed vacancies")
VACANCY_REMOVED_ID = 0
while VACANCY_REMOVED_ID < len(VACANCIES_REMOVED):
    vacancyisremoved(VACANCIES_REMOVED[VACANCY_REMOVED_ID], CSVFILENAME)
    VACANCY_REMOVED_ID += 1
