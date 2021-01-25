# This is a sample Python script.

import os
import sched
import time

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import psutil
import pymysql

schedule = sched.scheduler(time.time, time.sleep)
db = pymysql.connect(host="localhost", user="root", password="123", database="test", charset="utf8")
cursor = db.cursor()


def common_sql():
    # Use a breakpoint in the code line below to debug your script.
    mem = psutil.virtual_memory()
    query_sql = "select * from test;"
    insert_sql = "insert into test (`id`,`name`,`age`,`addr`) values (?,?,?,?)"
    del_sql = "delete from test where `id`=?"
    update_sql = "update test set 'addr' = ? where 'id'=?"
    try:
        cursor.execute(query_sql)
        results = cursor.fetchall()
        for row in results:
            ids = row[0]
            name = row[1]
            age = row[2]
            addr = row[3]
            print("%s,%s,%s,%s" % \
                  (ids, name, age, addr))
    except:
        print("Error:unable to fetch data")

    try:
        cursor2 = db.cursor()
        cursor2.execute(insert_sql, 3, "小吴", 26, "江西")
        db.commit()
    except:
        db.rollback()
        print("写入数据出错！")

    try:
        cursor3 = db.cursor()
        cursor3.execute(update_sql, '武汉', 1)
        db.commit()
    except:
        print('更新数据出错！')

    try:
        cursor4 = db.cursor()
        cursor4.execute(del_sql, 2)
    except:
        print('数据删除失败！')

    db.close()


def perform_command(inc):
    schedule.enter(inc, 0, perform_command, inc)
    back_mysql(database='', table='', isNotAll=True, user='root', pwd='123')


def timming_exe(inc=60):
    schedule.enter(inc, 0, perform_command, inc)
    schedule.run()


def back_mysql(database, table, isNotAll, user, pwd):
    now_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
    print('开始备份数据，当前时间为:', now_time)
    try:
        if isNotAll is False:
            if table is True:
                backup_database = 'mysqldump -u%s -p%s %s %s >/tmp/mysqlBackup/mysql_%s_%s_%s.sql' % (
                    user, pwd, database, table, database, table, now_time)
            else:
                backup_database = 'mysqldump -u%s -p%s %s >/tmp/mysqlBackup/mysql_%s_%s.sql' % (
                    user, pwd, database, database, now_time)
        else:
            backup_database = 'mysqldump -u%s -p%s -A>/tmp/mysqlBackup/mysql_all_backup_%s.sql' % (user, pwd, now_time)
        os.system(backup_database)
        print("数据库备份完成！")
    except:
        print('数据库备份失败！')


print("show time after 60 seconds:")
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    # timming_exe(60)
    common_sql()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
