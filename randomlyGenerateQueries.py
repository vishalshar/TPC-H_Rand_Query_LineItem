
import psycopg2
import random


# Generate random queries
###################
# L_ORDERKEY
# L_PARTKEY
# L_SUPPKEY
# L_LINENUMBER
# L_SHIPDATE
# L_DISCOUNT
# L_TAX


# L_COMMITDATE
# L_RECEIPTDATE
# L_QUANTITY
# L_EXTENDEDPRICE
###################



####################
## Set Parameters ##
####################
number_of_max_params = 4
list_of_columns = ['L_ORDERKEY','L_PARTKEY','L_SUPPKEY','L_LINENUMBER','L_SHIPDATE','L_DISCOUNT','L_TAX']

# Randomly sample columns of table
def getColumns():
    selectedColumn = random.sample(list_of_columns, number_of_max_params)
    return selectedColumn

def sampleDistinctValues(colname, cur):
    cur.execute("SELECT DISTINCT "+colname+" from lineitem")
    rows = cur.fetchall()
    return random.choice(rows)


try:
    
    # Connect to postgress local server
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='admin'")
    cur = conn.cursor()
    
    # operator selection 
    operator = ['=','>','<']

    # Randomly sample columns of table
    columns = getColumns()
    
    # Random Query Generator
    query = 'select count(*) from lineitem where '
    for column in columns:
        value = sampleDistinctValues(column, cur)
        #print value[0]
        #print type(value[0])
        #print type(value)
        query = query + column +' '+ random.choice(operator) +' '+ str(value[0]) +' and '

    query = query[:-4] # remove the last and added to string

    print query[:-4]


    # print "\nShow me the databases:\n"
    # select count(*) from lineitem where L_TAX < 0.07 and L_PARTKEY < 20987 and L_LINENUMBER = 3 and L_ORDERKEY > 1182



except:
    print "I am unable to connect to the database"

