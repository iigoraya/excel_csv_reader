import pandas as pd
import re
import pymysql
import os
import numpy as np

pymysql.install_as_MySQLdb()
database = pymysql.NULL
queries = list()


def init():
    os.system('find . -name ".DS_Store" -delete')
    initDbConnection()
    initFileRead()

def initDbConnection():
# * Initializing database
    global database
    database = pymysql.connect(
        # host="35.80.156.58", user="kingarthur", passwd="TheRoundTable20!", db="test_umair")
        host="localhost", user="root", passwd="password", db="test")

def initFileRead():

# * Insert query and other variables
    manufacturer = ''
    brand = ''
    cardSet = ''
    source = ''
    category = ''
    seq = ''
    fileNameList = list()
    queryParams = list()

    for (dirpath, dirnames, filenames) in os.walk('./crawlers/sports_data_csv', topdown=True):
        fileNameList.extend(filenames)
        break
    
    for filename in fileNameList:
        for encoding in ["utf-8-sig", "cp1252", "iso-8859-1", "latin1"]:
            try:        
                df = pd.read_csv(f"./crawlers/sports_data_csv/{filename}", header=0, names=None, index_col=False, keep_default_na=True, encoding=encoding)
                break
            except ValueError:
                print (f"Failed to read for encoding => {encoding}")
                continue
        print (f"Processing file {filename}")
        
# ?     Loop through each row in one file
        for index, row in df.iterrows():
            if (df.columns.__contains__('Card Set') and isinstance(row['Card Set'], str)):
                if (row['Card Set'].strip() == 'T-Minus 3'): 
                    continue
            if ((df.columns.__contains__('Number') and isinstance(row['Number'], str)) 
                or (df.columns.__contains__('#') and isinstance(row['#'], str))):
                continue

# !     Setting player
            player = f"{row['Subjects']}" if df.columns.__contains__('Subjects') else f"{row['Name']}" if df.columns.__contains__('Name') else f"{row['Player']}" if df.columns.__contains__('Player') else f"{row['First Name']} {row['Last Name']}" if df.columns.__contains__('First Name') else f"{row['Description']}" if df.columns.__contains__('Description') else f"{row['Driver']}" if df.columns.__contains__('Driver') else f"{row['cardName']}" if df.columns.__contains__('cardName') else ''
            player = replaceNan(player)
# !     Setting team
            team = f"{row['Team']}" if df.columns.__contains__('Team') else f"{row['Team City']} {row['Team Name']}" if df.columns.__contains__('Team Name') else row['cardTeam'] if df.columns.__contains__('cardTeam') else ''
            team = replaceNan(team)
# !     Setting card number
            num = f"{row['#']}" if df.columns.__contains__('#') else f"{row['Card']}" if df.columns.__contains__('Card') else f"{row['Number']}" if df.columns.__contains__('Number') else f"{row['cardNumber']}" if df.columns.__contains__('cardNumber') else 0
            cardNumber = 0 if (num == 'nan') else num

# !     Setting sequence
            seq_ = f"{row['Seq.']}" if df.columns.__contains__('Seq.') else f"{row['Form']}" if df.columns.__contains__('Form') else 0
            try:
                # seq = replaceNan(seq_)
                seq = getSequenceValueAsFloat(seq_)
            except ValueError:
                print (f"Invalid sequence value {seq_} of row {index+1}, expected float or int")

# !     Setting card set
            cardSet = f"{row['Card Set']}" if df.columns.__contains__('Card Set') else f"{row['Set Name']}" if df.columns.__contains__('Set Name') else f"{row['Set']}" if df.columns.__contains__('Set') else ''
            cardSet = '' if (cardSet == 'nan') else cardSet
            if 'Set' or 'set' in cardSet:
                cardSet.replace('Set', '')
                cardSet.replace('set', '')

# !     Setting card subset
            subset = f"{row['Subset']}" if df.columns.__contains__('Subset') else f"{row['Subset Name']}" if df.columns.__contains__('Subset Name') else 'Not Found'
            subset = replaceNan(subset)
# !     Setting card code
            cardCode = f"{row['Checklist']}" if df.columns.__contains__('Checklist') else f"{row['Card Code']}" if df.columns.__contains__('Card Code') else ''
            cardCode = '' if (cardCode == 'nan') else cardCode
            # cardCode = replaceNan(cardCode)
# !     Setting team city
            teamCity = f"{row['Team City']}" if df.columns.__contains__('Team City') else ''
            teamCity = replaceNan(teamCity)
# !     Setting team code
            teamCode = f"{row['Team Code']}" if df.columns.__contains__('Team Code') else ''
            teamCode = replaceNan(teamCode)
# !     Setting source
            source = f"{row['source']}" if df.columns.__contains__('source') else filename
            source = replaceNan(source)
# !     Setting year
            yearInFilename = f"{row['Year']}" if df.columns.__contains__('Year') else 'Not Found'
            yearInFilename = replaceNan(yearInFilename)
# !     Setting manufacturer
            manufacturer = f"{row['Mfr']}" if df.columns.__contains__('Mfr') else 'Not Found'
            manufacturer = replaceNan(manufacturer)
# !     Setting brand
            brand = f"{row['brand']}" if df.columns.__contains__('brand') else 'Not Found'
            brand = replaceNan(brand)
# *     Add insert query
            params = (cardNumber, player, team, cardSet, subset, yearInFilename, manufacturer, brand, seq, teamCity, teamCode, cardCode, source, category)
            queryParams.append(params)
#? Start database transactions
    initDatabaseTransactions(queryParams)
    quit()


def initDatabaseTransactions(queryParams):
    global database
    cursor = database.cursor()
    createTable(cursor)
    insertQuery = "INSERT INTO baseball_test (card_number,player,team,card_set, card_subset, year, manufacturer, brand, seq, team_city, team_code, card_code, source, category) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
# ? Insert row to database
    print ("Starting insert operation")
    cursor.executemany(insertQuery, queryParams)
    cursor.close()
# ? Commit database connection
    database.commit()


def createTable(cursor):
# * Create table if doesn't exist
    dropTableQuery = ("DROP TABLE IF EXISTS baseball_test")
    createTableQuery = ("CREATE TABLE IF NOT EXISTS baseball_test (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,card_number text,player text,team text,card_set text, card_subset text,seq int, year text, brand text, manufacturer text, team_city text, team_code text, card_code text, source text, category text);")
    cursor.execute(dropTableQuery)
    print ("Database table dropped")
    cursor.execute(createTableQuery)
    print ("Database table created")


# ? Validate sequence and return parsed value as float
def getSequenceValueAsFloat(seq_):
    if str(seq_).find("/") != -1:
        seq_ = int(str(seq_).rpartition('/')[-1])
    if str(seq_).find(":") != -1:
        seq_ = int(str(seq_).rpartition(':')[-1])
    if (seq_ == 'nan' or str(seq_).find("-") or not str(seq_).isdecimal()): 
        seq_ = 0
    return 0 if (pd.isna(float(seq_)) or seq_ == '' or seq_ == 'nan') else float(seq_)

def replaceNan(value):
    return str(value).replace('nan', '')

# ? Initialize script
init()
