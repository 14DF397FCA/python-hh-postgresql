import json
import requests
import csv
import os.path
import tempfile

VACANCIES_URL = 'https://api.hh.ru/vacancies?clusters=false&specialization=1&area=2&only_with_salary=true&per_page=100&page='
VACANCY_URL = "https://api.hh.ru/vacancies/"
CSVFILENAME = "pyTest.csv"

def getcsvpath():
    if tempfile.gettempdir() == ':':
        return tempfile.gettempdir() + "\\" + "pyTest.csv"
    else:
        return tempfile.gettempdir() + "/" + "pyTest.csv"

# Uncomment if you want save result file to temp directory
# CSVFILENAME = getcsvpath()

def createemptyfile():
    if os.path.exists(CSVFILENAME) == False:
        with open(CSVFILENAME, 'w') as outFile:
            outFile.write("")
            outFile.close()

createemptyfile()

RESP = requests.get(VACANCIES_URL + str(0))
DATA_LOADS = json.loads(RESP.text)
PAGECOUNT = DATA_LOADS['pages']
VACANCIES = {}
VACANCIES_REMOVED = {}
PAGE = 0
while PAGE < DATA_LOADS['pages']:
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
def readexistsvacancies():
    print("Read already exist vacancies")
    with open(CSVFILENAME, 'rt') as CSVFILE:
        VACANCIES_REMOVED_ID = 0
        rreader = csv.reader(CSVFILE, dialect='excel')
        for row in rreader:
            if row[0] in VACANCIES:
                del VACANCIES[row[0]]
            else:
                VACANCIES_REMOVED[VACANCIES_REMOVED_ID] = str(row[0])
                VACANCIES_REMOVED_ID += 1

readexistsvacancies()

# Add new vacancies
def writenewvacancies():
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
                    # VACANCY_KEY_SKILLS += VACANCY_DATA['key_skills'][KEY_SKILL]['name'].encode('utf-8') + ";".encode('utf-8')
                    VACANCY_KEY_SKILLS += VACANCY_DATA['key_skills'][KEY_SKILL]['name'] + ";"
                # VACANCY_COUNT = len(VACANCY_DATA['specializations'])
            # if VACANCY_COUNT > 0:
            # for PROFAREA in range(VACANCY_COUNT):
            # print(VACANCY_DATA['specializations'][PROFAREA]['profarea_id']
            # print(VACANCY_DATA['specializations'][PROFAREA]['profarea_name']
            # print(VACANCY_DATA['specializations'][PROFAREA]['id']
            # print(VACANCY_DATA['specializations'][PROFAREA]['name']
            if 'address''city' not in VACANCY_DATA:
                VACANCY_ADDRESS_CITY = "No data"
            else:
                VACANCY_ADDRESS_CITY = VACANCY_DATA['address']['city']
            if 'address''street' not in VACANCY_DATA:
                VACANCY_ADDRESS_STREET = "No data"
            else:
                VACANCY_ADDRESS_STREET = VACANCY_DATA['address']['street']
            if 'address''building' not in VACANCY_DATA:
                VACANCY_ADDRESS_BUILDING = "No data"
            else:
                VACANCY_ADDRESS_BUILDING = VACANCY_DATA['address']['building']
            if 'address''metro_stations''line_id' not in VACANCY_DATA:
                VACANCY_ADDRESS_METRO_LINE_ID = "No data"
                VACANCY_ADDRESS_METRO_LINE_NAME = "No data"
            else:
                VACANCY_ADDRESS_METRO_LINE_ID = VACANCY_DATA['address']['metro_stations']['line_id'],
                VACANCY_ADDRESS_METRO_LINE_NAME = VACANCY_DATA['address']['metro_stations']['line_name'],
            if 'address''metro_stations''station_id' not in VACANCY_DATA:
                VACANCY_ADDRESS_METRO_STATION_ID = "No data"
                VACANCY_ADDRESS_METRO_STATION_NAME = "No data"
            else:
                VACANCY_ADDRESS_METRO_STATION_ID = VACANCY_DATA['address']['metro_stations']['station_id']
                VACANCY_ADDRESS_METRO_STATION_NAME = VACANCY_DATA['address']['metro_stations']['station_name']
            if 'employer''id' not in VACANCY_DATA:
                VACANCY_EMPLOYER_ID = "No data"
                VACANCY_EMPLOYER_NAME = "No data"
            else:
                VACANCY_EMPLOYER_ID = VACANCY_DATA['employer']['id']
                VACANCY_EMPLOYER_NAME = VACANCY_DATA['employer']['name']
            print(VACANCY_URL + str(VACANCY_LOADS['id']))
            wwriter.writerow([
                VACANCY_LOADS['id'],
                VACANCY_LOADS['name'],
                VACANCY_DATA['alternate_url'],
                str(VACANCY_DATA['premium']),
                VACANCY_DATA['description'],
                VACANCY_DATA['schedule']['id'],
                VACANCY_DATA['schedule']['name'],
                VACANCY_DATA['experience']['id'],
                VACANCY_DATA['experience']['name'],
                VACANCY_ADDRESS_CITY,
                VACANCY_ADDRESS_STREET,
                VACANCY_ADDRESS_BUILDING,
                VACANCY_ADDRESS_METRO_LINE_ID,
                VACANCY_ADDRESS_METRO_LINE_NAME,
                VACANCY_ADDRESS_METRO_STATION_ID,
                VACANCY_ADDRESS_METRO_STATION_NAME,
                VACANCY_KEY_SKILLS,
                VACANCY_DATA['employment']['id'],
                VACANCY_DATA['employment']['name'],
                VACANCY_DATA['salary']['to'],
                VACANCY_DATA['salary']['from'],
                VACANCY_DATA['salary']['currency'],
                VACANCY_DATA['area']['id'],
                VACANCY_DATA['area']['name'],
                VACANCY_EMPLOYER_ID,
                VACANCY_EMPLOYER_NAME,
                "Active"
            ])

writenewvacancies()

# Mark removed vacancies
print("Mark removed vacancies")
VACANCY_REMOVED_ID = 0
while VACANCY_REMOVED_ID < len(VACANCIES_REMOVED):
    vacancyisremoved(VACANCIES_REMOVED[VACANCY_REMOVED_ID], CSVFILENAME)
    VACANCY_REMOVED_ID += 1
