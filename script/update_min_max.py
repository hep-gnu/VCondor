#!/usr/bin/env phthon
# update_min_max.py - Update the Min or Max value for specific queue.

import MySQLdb, sys

def get_db_con(dbname="resource_pool"):
    DB_HOST="xxx"
    DB_USER="xxx"
    DB_PASS="xxx"
    conn=MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASS,db=dbname)
    return conn


def update_res_min_max(con,queuename,min,max):
    con.ping(True)
    cursor = con.cursor()
    try:
        sql = "update resource set min = %s, max = %s where name='%s'"%(min,max,queuename)
        cursor.execute(sql)
        con.commit()
        print "update success"
    except:
        con.rollback()

    cursor.close()
    con.close()

def Usage():
    print "Usage: python update_min_max.py <queuename> <min> <max>"

def main():
    if len(sys.argv) < 4:
        Usage()
        sys.exit()
    
    queuename = sys.argv[1].upper()
    min = sys.argv[2]
    max = sys.argv[3]
    
    con = get_db_con()
    update_res_min_max(con,queuename,min,max)

if __name__ == '__main__':
    main()
