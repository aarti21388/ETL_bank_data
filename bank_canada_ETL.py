import petl 
import configparser 
import requests 
import datetime 
import sys,json
import pymysql
#get data from configuration file 


config=configparser.ConfigParser()

# request data from URL
def read_api_data(url,startDate):
    try:
        res=requests.get(url+startDate)   
    except Exception as e:
        print("could not make request"+str(e))
        sys.exit()

    # initialize list of lists for data storage
    BOCDates=[]
    BOCRATES=[]

    # check response status and process BOC JSON object
    if res.status_code==200:
        
        BOCRaw=json.loads(res.text)

         # extract observation data into column arrays
        for row in BOCRaw["observations"]:
            BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
            BOCRATES.append(float(row["FXUSDCAD"]["v"]))
        
         # create petl table from column arrays and rename the columns
        exchangeRates=petl.fromcolumns([BOCDates,BOCRATES],header=['date','rate'])

    return exchangeRates

#read Expense excel file
def read_excel(filename,sheet_name):
    try:
        # load expense excel document
        expenses=petl.fromxlsx(filename,sheet=sheet_name)
        return expenses
    except Exception as e:
        print("count not read"+str(e))
        sys.exit()

# intialize database connection
def intialize_connection(destserver,username,password,destDatabase):  
    try:
        
        connection = pymysql.connect(host=destserver,user=username,password=password, database=destDatabase)
        # tell MySQL to use standard quote character
        cursor=connection.cursor()
        cursor.execute('SET SQL_MODE=ANSI_QUOTES')  
        #cursor.execute('delete from Expenses') 
        
        return cursor,connection
    except Exception as e:
        print('Couldnt connect to database'+ str(e))
        sys.exit()
      
     # populate Expenses database table 
def populate_table(expenses,connection):
    try:
        petl.io.todb(expenses,connection,'Expenses')
        return connection
    except Exception as e:
        print('Error while creating table '+ str(e))
        sys.exit()

def main():
    try:
    #read the contents of the configuration files
        config.read('etl.ini')
    except Exception as e:
        print("count not read configuration fil:" +str(e))
        sys.exit()

    # read settings from configuration file

    startDate=config['CONFIG']['startDate']
    url=config['CONFIG']['url']
    destserver=config['CONFIG']['server']
    destDatabase=config['CONFIG']['database']
    username=config['CONFIG']['username']
    password=config['CONFIG']['password']

    exchangeRates=read_api_data(url,startDate)

    expenses=read_excel("Expenses.xlsx","Github")

   
    # Join Tables with common field 'date'
    expenses=petl.outerjoin(exchangeRates,expenses,key='date')
    
    #fill down missing value
    expenses=petl.filldown(expenses,'rate')
   
    # remove dates with no expenses
    expenses=petl.select(expenses,lambda x:x.USD !=None)
    
    # Calculate the 'CAD' column using a lambda function
    expenses = petl.addfield(expenses, 'CAD', lambda row: row.USD * row.rate)
     
    cursor,connection=intialize_connection(destserver,username,password,destDatabase)
    
    connection=populate_table(expenses,connection)
    cursor.close()
    connection.close()

if __name__=='__main__':
    main()