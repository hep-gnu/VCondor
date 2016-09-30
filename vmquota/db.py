#!/usr/bin/env phthon
import MySQLdb, os, commands,re, time

def get_db_con(dbname="resource_pool"):
    DB_HOST="localhost"
    DB_USER="root"
    DB_PASS="virtcompute"
    try:
        conn=MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASS,db=dbname)
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return

    return conn

def get_res_info(con,queuename):
    ''' get an queue ifno by its name, ie [name,min,running,max]'''
    res_info = []
    con.ping(True)
    cursor = con.cursor()
    cursor.execute("select name,min,max,running,available,reserve_time from resource where name = '%s'"%queuename)
    rows = cursor.fetchall()
    cursor.close()
    for row in rows:
        print row
        res_info.append(row[0])
        res_info.append(row[1])
        res_info.append(row[2])
        res_info.append(row[3])
        res_info.append(row[4])
        res_info.append(row[5])
    return res_info

def update_res_running(con,queuename,running):
    con.ping(True)
    cursor = con.cursor()
    cursor.execute("update resource set running = %s where name='%s'"%(running,queuename))
    cursor.close()

def update_res_avail(con,queuename,avail,allocate_time):
    con.ping(True)
    cursor = con.cursor()
    cursor.execute("update resource set available=%s,reserve_time='%s' where name='%s'"%(avail,allocate_time,queuename))
    cursor.close()

if __name__ == '__main__':
    con = get_db_con()
