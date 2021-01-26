# This is a sample Python script.

import os
import sched
import time
import logging

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from logging import handlers

import pymysql


class Logger:
    schedule = sched.scheduler(time.time, time.sleep)
    db = pymysql.connect(host="localhost", user="root", password="123", database="test", charset="utf8")
    cursor = db.cursor()

    def __init__(self):
        self.logger = logging.getLogger('mysql_backup.log')
        format_str = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename='mysql_backup.log', when='D', backupCount=20, encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)

    def common_sql(self):
        # Use a breakpoint in the code line below to debug your script.
        query_sql = "select * from test;"
        insert_sql = "insert into test (`name`,`age`,`addr`) values (%s,%s,%s)"
        del_sql = "delete from test where `id`=%s"
        update_sql = "update test set `addr` = %s where 'id'=%s"
        try:
            self.cursor.execute(query_sql)
            results = self.cursor.fetchall()
            for row in results:
                ids = row[0]
                name = row[1]
                age = row[2]
                addr = row[3]
                print("%s,%s,%s,%s" % \
                      (ids, name, age, addr))
        except ValueError:
            print("Error:unable to fetch data")

        try:
            execute = self.cursor.execute(insert_sql, args=("小吴3", "26", "江西"))
            self.db.commit()
        except ValueError:
            self.db.rollback()
            print("写入数据出错！")

        try:
            self.cursor.execute(update_sql, args=('武汉', 1))
            self.db.commit()
        except ValueError:
            print('更新数据出错！')

        try:
            self.cursor.execute(del_sql, args=2)
            self.db.commit()
        except ValueError:
            self.db.rollback()
            print('数据删除失败！')

        self.db.close()
        exit(0)

    def perform_command(self, inc):
        self.schedule.enter(inc, 0, self.perform_command, inc)
        self.back_mysql(database='', table='', isNotAll=True, user='root', pwd='123')

    def timming_exe(self, inc=60):
        self.schedule.enter(inc, 0, self.perform_command, inc)
        self.schedule.run()

    def back_mysql(self, database, table, isNotAll, user, pwd):
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
                backup_database = 'mysqldump -u%s -p%s -A>/tmp/mysqlBackup/mysql_all_backup_%s.sql' % (
                    user, pwd, now_time)
            os.system(backup_database)
            print("数据库备份完成！")
        except:
            print('数据库备份失败！')

    print("show time after 60 seconds:")
    # Press the green button in the gutter to run the script.


if __name__ == '__main__':
    # print_hi('PyCharm')
    # timming_exe(60)
    # common_sql()
    log = Logger()
    log.logger.debug("debug测试")
    log.logger.info("info测试")
    log.logger.warning("warning测试")
    log.logger.error("error测试")
    log.logger.critical("严重")

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
