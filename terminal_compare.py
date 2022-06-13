"""
    Compare db schema structures

    args:
        1 - schema name
"""
import sys
import pymysql
from pprint import pprint
import re

def getListTablesInSchema(db) -> list:
    cursor = db.cursor()
    query = "SHOW TABLES;"
    cursor.execute(query)
    #db.commit()

    tables_list = []
    for row in cursor:
        tables_list.append(row[0])

    return tables_list

def getMissingElemList(list1: list, list2: list) -> list:
    tables_missing = []
    for t in list1:
            if t not in list2:
                tables_missing.append(t)
    return tables_missing

def getDDLTable(db, table_name: str) -> str:
    cursor = db.cursor()
    query = "SHOW CREATE TABLE {};".format(table_name)
    cursor.execute(query)
    row = cursor.fetchone()
    return row[1]

def getInfoTable(db, table_name: str) -> dict:
    """
        SHOW FULL COLUMNS FROM job;   -- colonne
        SHOW INDEX FROM colori_parti; -- indici
    """
    cursor = db.cursor()
    query = "SHOW FULL COLUMNS FROM {};".format(table_name)
    cursor.execute(query)
    column_info = {}

    for row in cursor:
        column_info[row["Field"]] = {
            "field": row["Field"],
            "type": row["Type"],
            "collation": row["Collation"],
            "null": row["Null"],
            "key": row["Key"],
            "default": row["Default"],
            "extra": row["Extra"],
            "privileges": row["Privileges"],
            "comment": row["Comment"]
        }
    return column_info

def isDDLEqual(ddl1, ddl2):
    """
        Should be more complex
    """
    return ddl1 == ddl2

def parseColumnsInfo(ddl) -> dict:
    columns = {}
    ddl_split = ddl.split("\n")

    for ddl_col in ddl_split[1:-1]:
        res = re.findall(r'^[ ]*`([\w\d\-\_]*)`', ddl_col)
        if len(res) > 0:
            columns[res[0]] = ddl_col

    return columns

def printPrettyOutput(missing_left: list, missing_right: list, ddl_delta: dict):
    tab_size = 10
    max_table_name = 0

    for t in missing_left:
        if len(t.strip()) > max_table_name:
            max_table_name = len(t)

    print("len", max_table_name)
    # header
    print("Tabelle mancanti".center(max_table_name*2 + 3, ' '))
    print('-' * (max_table_name*2 + 3))
    # sx
    print("Sinistra".ljust(max_table_name + 1, ' ') + "|".ljust(max_table_name + 1, ' '))
    print('-' * (max_table_name + 1) + "|".ljust(max_table_name + 2, '-'))
    for t in missing_left:
        print(t.strip() + " ".rjust(max_table_name - len(t.strip()) + 1) + "|".ljust(max_table_name + 1))
    # dx
    print('-' * (max_table_name*2 + 3))
    print("|".rjust(max_table_name + 2, ' ') + "Destra".rjust(max_table_name + 1, ' '))
    print('-' * (max_table_name + 1) + "|".ljust(max_table_name + 1, '-'))
    for t in missing_right:
        print("|".rjust(max_table_name + 2) + t.rjust(max_table_name + 3, ' '))

    print('-' * (max_table_name*2 + 3))
    print("Tables diff")
    print('-' * (max_table_name*2 + 3))

    for t in ddl_delta:
        print(t.strip())
        for col in ddl_delta[t]:
            print('|')
            print('|' + '_' * tab_size + col.strip())
            str_diff = " // ".join(ddl_delta[t][col])
            print(' ' * (tab_size+1) + '|')
            print(' ' * (tab_size+1) + '|' + '_' * (tab_size) + str_diff)
            print('') # line break
            


# Script

if len(sys.argv) < 2:
    exit("Check args")

try:
    db_name = sys.argv[1]

    db = pymysql.connect(
        host="192.168.91.16", 
        user="root", 
        password="s83dcpix!", 
        database=db_name
    )

    db2 = pymysql.connect(
        host="172.31.212.11", 
        user="root", 
        password="s83dcpix!", 
        database=db_name
    )

    tables_missing_left = []
    tables_missing_right = []
    tables_ddl_delta = {}

    try:
        tables = getListTablesInSchema(db)
        tables2 = getListTablesInSchema(db2)

        tables_missing_right = getMissingElemList(tables, tables2)
        tables_missing_left = getMissingElemList(tables2, tables)

        tables.extend(tables2)
        for t in tables:
            if t not in tables_missing_left and t not in tables_missing_right:
                ddl_l = getDDLTable(db, t)
                ddl_r = getDDLTable(db2, t)
                if not isDDLEqual(ddl_l, ddl_r):
                    ddl_split_l = parseColumnsInfo(ddl_l)
                    ddl_split_r = parseColumnsInfo(ddl_r)
                    delta = {}
                    for col in ddl_split_l:
                        if col not in ddl_split_r:
                            # Missing column right
                            delta[col] = (ddl_split_l[col], "")
                        elif ddl_split_l[col] != ddl_split_r[col]:
                            # Diff ddl between columns
                            delta[col] = (ddl_split_l[col], ddl_split_r[col])

                    for col in ddl_split_r:
                        if col not in delta and col not in ddl_split_l:
                            # Missing column left
                            delta[col] = ("", ddl_split_r[col])

                    if delta: 
                        tables_ddl_delta[t] = delta

    except pymysql.Error as e:
        print(e)
    
    # Print result
    # print("Tables missing left")
    # pprint(tables_missing_left)
    # print("Tables missing right")
    # pprint(tables_missing_right)

    printPrettyOutput(tables_missing_left, tables_missing_right, tables_ddl_delta)

    # print("Tables diff")
    # pprint(tables_ddl_delta)

except pymysql.err.InternalError as ie:
    exit(str(ie))



