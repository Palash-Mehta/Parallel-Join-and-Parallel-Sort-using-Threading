#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import threading
import psycopg2
import os
import sys


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    no_of_threads = 5
    threads = [0 for i in range(no_of_threads)]

    cur.execute("select max(" + SortingColumnName + ") from " +InputTable)
    max_value = cur.fetchone()[0]
    cur.execute("select min(" + SortingColumnName + ") from " +InputTable)
    min_value = cur.fetchone()[0]
    interval = float(max_value - min_value)/float(no_of_threads)

    for i in range(0,no_of_threads):
        lower_bound = min_value + interval * i
        upper_bound = lower_bound + interval
        threads[i] = threading.Thread(target = sort, args=(InputTable, SortingColumnName, i, lower_bound, upper_bound, openconnection))
        threads[i].start()

    cur.execute("create table " + OutputTable + "(like " + InputTable+ ")")

    for i in range(0,no_of_threads):
        threads[i].join()
        table = 'sort_table'+str(i)
        cur.execute("insert into " + OutputTable + " select * from " +table)
    openconnection.commit()

def sort(InputTable, SortingColumnName, i, lower_bound, upper_bound, openconnection):
    cur = openconnection.cursor()
    table = 'sort_table' + str(i)
    cur.execute("create table " + table + " ( like " + InputTable + ")")
    if i == 0:
        cur.execute("insert into " + table + " select * from " + InputTable + " where " + SortingColumnName + ">=" + str(lower_bound) + " and " + SortingColumnName + "<=" +str(upper_bound) + " order by " + SortingColumnName + " asc ")
    else:
        cur.execute("insert into " + table + " select * from " + InputTable + " where " + SortingColumnName + ">" + str(lower_bound) + " and " + SortingColumnName + "<=" +str(upper_bound) + " order by " + SortingColumnName + " asc ")



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    no_of_threads = 5
    threads = [0 for i in range(no_of_threads)]

    cur.execute("select min(" + Table1JoinColumn + ") from " + InputTable1)
    min1 = cur.fetchone()[0]
    cur.execute("select max(" + Table1JoinColumn + ") from " + InputTable1)
    max1 = cur.fetchone()[0]
    cur.execute("select min(" + Table2JoinColumn + ") from " + InputTable2)
    min2 = cur.fetchone()[0]
    cur.execute("select max(" + Table2JoinColumn + ") from " + InputTable2)
    max2 = cur.fetchone()[0]

    min_value = min(min1,min2)
    max_value = max(max1,max2)

    interval = float(max_value-min_value)/float(no_of_threads)

    cur.execute("select column_name,data_type from information_schema.columns where table_name = '" + InputTable1 + "'")
    input_table1_schema = cur.fetchall()

    cur.execute("select column_name,data_type from information_schema.columns where table_name = '" + InputTable2 + "'")
    input_table2_schema = cur.fetchall()

    for i in range(0,no_of_threads):
        lower_bound = min_value + interval * i
        upper_bound = lower_bound + interval
        threads[i] = threading.Thread(target = join, args=(InputTable1,InputTable2,i,input_table1_schema,input_table2_schema,Table1JoinColumn,Table2JoinColumn,lower_bound,upper_bound,openconnection))
        threads[i].start()

    cur.execute("create table " + OutputTable + " ( like " + InputTable1 + ")")

    for i in range(0,len(input_table2_schema)):
        cur.execute("alter table " + OutputTable + " add column " + input_table2_schema[i][0] + " " + input_table2_schema[i][1])
    for i in range(no_of_threads):
        threads[i].join()
        table = 'table_output'+str(i)
        cur.execute("insert into " + OutputTable + " select * from " + table)
        openconnection.commit()

def join(InputTable1,InputTable2,i,InputTable1_schema,InputTable2_schema,Table1JoinColumn,Table2JoinColumn,lower_bound,upper_bound,openconnection):
    cur = openconnection.cursor()

    table1_name = 'table1_name' + str(i)
    cur.execute("create table " + table1_name + " ( like " + InputTable1 + ")")
    table2_name = 'table2_name' + str(i)
    cur.execute("create table " + table2_name + " ( like " + InputTable2 + ")")
    table = 'table_output'+str(i)
    cur.execute("create table " + table + " ( like " + InputTable1 + ")")

    for j in range(0,len(InputTable2_schema)):
        cur.execute("alter table " + table + " add column " + InputTable2_schema[j][0] + " " + InputTable2_schema[j][1])

    if i==0:
        cur.execute("insert into " + table1_name + " select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(lower_bound) + " and " + Table1JoinColumn + " <= " + str(upper_bound))
        cur.execute("insert into " + table2_name + " select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(lower_bound) + " and " + Table2JoinColumn + " <= " + str(upper_bound))
    else:
        cur.execute("insert into " + table1_name + " select * from " + InputTable1 + " where " + Table1JoinColumn + " > " + str(lower_bound) + " and " + Table1JoinColumn + " <= " + str(upper_bound))
        cur.execute("insert into " + table2_name + " select * from " + InputTable2 + " where " + Table2JoinColumn + " > "+ str(lower_bound) + " and " + Table2JoinColumn + " <= " + str(upper_bound) + " order by " + Table2JoinColumn + " asc ")
    cur.execute("insert into " + table + " select * from " + table1_name + " join " + table2_name + " on " + table1_name + "." + Table1JoinColumn + " = " + table2_name + "." + Table2JoinColumn)



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
