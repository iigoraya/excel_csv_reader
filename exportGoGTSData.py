import traceback
import os
import pandas as pd
import re
import pymysql

from helpers import constants

pymysql.install_as_MySQLdb()
cursor = pymysql.NULL
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
        host="35.80.156.58", user="kingarthur", passwd="TheRoundTable20!", db="test_umair")
        # host="localhost", user="root", passwd="", db="test")

# ? Method that reads and stores all specified fields in database from every excel file in a specified directory
def initFileRead():
    global database
    fileNameList = []

# * Insert query and other variables
    manufacturer = ''
    brand = ''
    cardSet = ''
    source = ''
    category = ''

# ! Getting all excel filenames from directory 
    for (dirpath, dirnames, filenames) in os.walk('./superBreak/xls', topdown=True):
        fileNameList.extend(filenames)
        break

    for (dirpath, dirnames, filenames) in os.walk('./superBreak/csv', topdown=True):
        fileNameList.extend(filenames)
        break
    dfs = list()
    queryParams = list()
    try:
# ? Loop through each file
        for filename in fileNameList:
            print (f"Processing file {filename}")
            df = None
            if (filename.endswith(('.csv'))):
# ? Read csv
                try:
                    for encoding in constants.encodings:
                        df = pd.read_csv("./superBreak/csv/" + filename, header=0, names=None, index_col=False, keep_default_na=True, encoding=encoding)
                        break
                except ValueError:
                    print (f"Failed to read {filename} for encoding => {encoding}")
                    continue
# ? Read excel sheet
            else:
                df = pd.read_excel("./superBreak/xls/" + filename, header=0, names=None, index_col=False, keep_default_na=True)
            df['__id'] = 0
            dfs.append(df)
# ! Setting year
            yearInFilename = None
            try:
                yearInFilename = re.search('[0-9]+', filename).group()
            except AttributeError:
                yearInFilename = re.search('[0-9]+', filename)

# ! Setting category
            categoryIndex = 0
            for cat in constants.categories:
                if cat.lower() in filename.lower():
                    category = cat
                    filenameStrings = filename.lower().split("-")
                    categoryIndex = filenameStrings.index(cat.lower())
                    break
                else: category = ''

# ! Setting manufacturer
            for man in constants.manufacturerList:
                if man.lower() in filename.lower().replace("-", " "):
                    manufacturer = man
                    break
                else: manufacturer = ""

# ! Setting brand
            arrayOfStringsInFilename = list()
            arrayOfStringsInFilename = filename.split("-")
            categoryIndex = len(arrayOfStringsInFilename) - 1 if categoryIndex == 0 else categoryIndex
            
            if not yearInFilename:
                brand = ' '.join(arrayOfStringsInFilename[0:categoryIndex])
                print(f"FILENAME YEAR NOT PRESENT, Brand IS ==>> {brand}")
            elif (bool(re.search(r'\d', arrayOfStringsInFilename[1]))):
                brand = ' '.join(arrayOfStringsInFilename[2:categoryIndex])
            else:
                if categoryIndex <= 2:
                    categoryIndex = 3
                brand = ' '.join(arrayOfStringsInFilename[1:categoryIndex])

# ? Loop through each row in one file
            for index, row in df.iterrows():
                if (df.columns.__contains__('Card Set') and isinstance(row['Card Set'], str)):
                    if (row['Card Set'].strip() == 'T-Minus 3'): 
                        continue

                if ((df.columns.__contains__('Number') and isinstance(row['Number'], str)) 
                    or (df.columns.__contains__('#') and isinstance(row['#'], str))):
                    continue

# ! Setting player
                player = f"{row['Subjects']}" if df.columns.__contains__('Subjects') else f"{row['Name']}" if df.columns.__contains__('Name') else f"{row['Player']}" if df.columns.__contains__('Player') else f"{row['First Name']} {row['Last Name']}" if df.columns.__contains__('First Name') else f"{row['Description']}" if df.columns.__contains__('Description') else f"{row['Driver']}" if df.columns.__contains__('Driver') else ''

# ! Setting team
                team = f"{row['Team']}" if df.columns.__contains__('Team') else f"{row['Team City']} {row['Team Name']}" if df.columns.__contains__('Team Name') else ''

# ! Setting card number
                num = f"{row['#']}" if df.columns.__contains__('#') else f"{row['Card']}" if df.columns.__contains__('Card') else f"{row['Number']}" if df.columns.__contains__('Number') else 0
                cardNumber = 0 if (pd.isna(num) or num == 'nan') else num

# ! Setting sequence
                seq_ = f"{row['Seq.']}" if df.columns.__contains__('Seq.') else f"{row['Form']}" if df.columns.__contains__('Form') else 0
                try:
                    seq = getSequenceValueAsFloat(seq_)
                except ValueError:
                    print (f"Invalid sequence value {seq_} of row {index+1}, expected float or int")

# ! Setting card set
                cardSet = f"{row['Card Set']}" if df.columns.__contains__('Card Set') else f"{row['Set Name']}" if df.columns.__contains__('Set Name') else f"{row['Set']}" if df.columns.__contains__('Set') else ''
                cardSet = '' if (pd.isna(cardSet) or cardSet == 'nan') else cardSet
                if 'Set' or 'set' in cardSet:
                    cardSet.replace('Set', '')
                    cardSet.replace('set', '')

# ! Setting card subset
                subset = f"{row['Subset']}" if df.columns.__contains__('Subset') else ''

# ! Setting card code
                cardCode = f"{row['Checklist']}" if df.columns.__contains__('Checklist') else f"{row['Card Code']}" if df.columns.__contains__('Card Code') else ''
                cardCode = '' if (pd.isnull(cardCode)) else cardCode

# ! Setting team city
                teamCity = f"{row['Team City']}" if df.columns.__contains__('Team City') else ''

# ! Setting team code
                teamCode = f"{row['Team Code']}" if df.columns.__contains__('Team Code') else ''

# ! Setting source
                source = f"{row['source']}" if df.columns.__contains__('source') else filename

# * Add insert query
                params = (cardNumber, player, team, cardSet, subset, yearInFilename, manufacturer, brand, seq, teamCity, teamCode, cardCode, source, category)
                queryParams.append(params)

# ? Export data to single excel file
        try:
            os.system('mkdir excelSheetByManufacturer')
        except FileExistsError:
            print ("directory already exists")

    # * Define excel writer and output file
        Excelwriter = pd.ExcelWriter("./excelSheetByManufacturer/superBreak.xlsx",engine="xlsxwriter")
    # * Concatenate list of dataframes
        finalDf = pd.concat(dfs)
    # * Write to file
        try:
            finalDf.to_excel(Excelwriter, sheet_name="CardSheet",index=False, encoding="UTF-8")
        except ValueError:
            print("Error while saving all data frames to single excel sheet.")
            finalDf.to_csv('./excelSheetByManufacturer/superBreak.csv', sep=",", index=False)
    # * Save file
        Excelwriter.save()

#? Start database transactions
        initDatabaseTransactions(queryParams)
# ! Exit process
        quit()
    except Exception:
        print (f"\n\nError proccessing {filename}\n\n")
        print (Exception.with_traceback())

def initDatabaseTransactions(queryParams):
    global database
    cursor = database.cursor()
    createTable(cursor)
    insertQuery = "INSERT INTO superBreak (card_number,player,team,card_set, card_subset, year, manufacturer, brand, seq, team_city, team_code, card_code, source, category) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
# ? Insert row to database
    print ("Starting insert operation")
    cursor.executemany(insertQuery, queryParams)
    cursor.close()
# ? Commit database connection
    database.commit()

def createTable(cursor):
# * Create table if doesn't exist
    dropTableQuery = ("DROP TABLE IF EXISTS superBreak")
    createTableQuery = ("CREATE TABLE IF NOT EXISTS superBreak (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,card_number text,player text,team text,card_set text, card_subset text,seq int, year text, brand text, manufacturer text, team_city text, team_code text, card_code text, source text, category text);")
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

# ? Initialize script
init()