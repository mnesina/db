__author__ = 'mar'
# coding=utf-8
try:
    import MySQLdb as mysql_conn
except:
    import mysql.connector as mysql_conn


#import cgi, sys # cgi.escape is deprecated use html.escape instead
import html, sys


## Утановка connector - an installation of connector
## sudo pip install --allow-all-external mysql-connector-python
## NB - при этом pip установлен именно как pip для python3
## NB - pip should be installed as pip for python3

class db(object):
    """
    Класс работы с базой данных mysql включает коннект к базе и основные методы для работы
    Class for to work with mysql database, including connection and main methods
    """
    def __init__(self, settings, with_transactions=False, raise_on_warnings=False):
        """ Inits db class
        :param settings: словарь конфига для доступа к бд
                - a dictionary of config to access a database
        :param  with_transactions - с транзакциями, или без. Если транзакции разрешены, то нужно из вызывающего файла объявлять begin и rollback/commit, если нет, включаем autocommit
               -
        :param raise_on_warnings: будут ли предупреждения вызывать исключения (по-умолчанию, как и в модуле, False)
                - declare if the warnings would raise an exception (False by default as it is set in connector module)
        """

        self.__withTransactions = with_transactions

        # определяем какая библиотека подключена
        try:
            ver = mysql_conn.__version_info__
            print('connector')
            self.__dblib = 'connector'
        except:
            print('MySQLdb')
            self.__dblib = 'MySQLdb'

        # дополняем конфиг:
        # adding configuration settings

        # для connector - for connector
        if self.__dblib == 'connector':
            settings['raise_on_warnings'] = raise_on_warnings
            # Переключение на Connector/Python C Extension (если это возможно)
            # Switch to Connector/Python C Extension (if possible)
            # см. документацию - see documentation:
            # https://dev.mysql.com/doc/connector-python/en/connector-python-cext-development.html
            if ver > (2, 1) and mysql_conn.HAVE_CEXT:
                #print('use_pure:False', '\n')
                settings['use_pure'] = False

        # для MySQLdb - for MySQLdb
        if self.__dblib == 'MySQLdb' :
            try:
                settings['password']
                passwd = settings['password']
                del settings['password']
                settings['passwd'] = passwd
            except:
                try:
                    settings['passwd']
                except:
                    print("Error:", sys.exc_info()[0])
                    raise

            try:
                dbname = settings['database']
                del settings['database']
                settings['db'] = dbname
            except:
                try:
                    settings['db']
                except:
                    print("Error:", sys.exc_info()[0])
                    raise

            settings['use_unicode'] = True,
            settings['charset'] = "utf8"
            settings['init_command']='SET NAMES UTF8'

        if (self.__withTransactions==False):
            settings['autocommit'] = True

        self.__connect(settings)

    def __connect(self, settings):
        """
        собственно соединение с базой
        connection to a database
        :param settings: словарь конфига для доступа к базе данных
        :return:
        """

        try:
            # создаем соединение с базой данных, используя конфиг
            # create database connection using configuration settings
            self.cnx = mysql_conn.connect(**settings)
            if(self.__withTransactions):
                self.begin()

        except:
            print("Error:", sys.exc_info()[0])
            raise

    def __query(self, query, type='SELECT'):
        """
        Создается курсор и выполняется запрос к базе
        Create cursor and execute a query to a database
        :param query: запрос, подготовленный в public фукциях - query, prepared in public functions
        :param type:
        :return: объект - курсор object - cursor
        """
        try:
            cursor = self.cnx.cursor()
            '''
            if self.__dblib == 'MySQLdb':
                cursor.execute('SET NAMES utf8')
                cursor.execute('SET CHARACTER SET utf8')
                cursor.execute('SET character_set_connection=utf8')
            '''
            cursor.execute(query)

            if type=='SELECT' :
                return cursor
            elif type=='INSERT' :
                id = cursor.lastrowid
                ##if id > 0 :
                   ##self.commit()
                    #self.cnx.commit()
                cursor.close
                return id
            else:
                ##self.commit()
                #self.cnx.commit()
                cursor.close

        except:
            print("Error:", sys.exc_info()[0])
            raise

    def getSelect(self, query):
        """
        NB! в этой модификации возвращаются только поля со значениями не null (None)
        http://javacoffee.de/?p=953
        :param query:
        :return:
        """

        out = []
        try:
            ##cursor = self.cnx.cursor()
            ##cursor.execute(query)
            cursor = self.__query(query)
            rows = cursor.fetchall()
            for row in rows:
                data = {}
                for i in range(len(row)):
                    if row[i] != None:
                        tmp = cursor.description
                        data[tmp[i][0]] = row[i]

                out.append(data)

        except Exception as err:
            # Your exception handling...
            print("Error in fetchassoc:")
            print(err)

        finally:
            cursor.close
            return out

    def getInfo(self,query):
        """

        :param query:
        :return:
        """
        res_arr = self.getSelect(query)
        if len(res_arr) > 0 :
            result = res_arr[0]
        else:
            result = False
        return result

    def getElement(self,query):
        """

        :param query:
        :return:
        """
        result = self.getInfo(query)
        for field in result:
            return result[field]

    def addInfo(self, data, table, ignore=''):
        """

        :param data:
        :param table:
        :param ignore '' or 'IGNORE'
        :return: id записи или ошибку
        """
        #https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-transaction.html
        #new_str = db.converter.escape('string to be escaped') - http://stackoverflow.com/questions/7540803/escaping-strings-with-python-mysql-connector
        add_tmp = []
        add_tmp_val = []

        for key in data :
            tmp = self.__escape(data[key])
            add_tmp.append('`%s`' % (key))
            add_tmp_val.append('\'%s\'' % (tmp))
        add_str = ','.join(add_tmp)
        add_val_str = ','.join(add_tmp_val)
        query = 'INSERT %s INTO  %s (%s) VALUES (%s)' % (ignore,table,add_str,add_val_str)

        #print(query,' ')

        return self.__query(query,'INSERT') #id

    def setInfo(self, data, table, value, column='ID'):
        """

        :param data:
        :param table:
        :param value:
        :param column:
        :return:
        """
        add_tmp = []

        for key in data :
            tmp = self.__escape(data[key])
            add_tmp.append('`%s`=\'%s\'' % (key,tmp))

        add_str = ','.join(add_tmp)
        query = 'UPDATE  %s SET %s WHERE %s=\'%s\'' %  (table, add_str, column, str(value))
        #query = u'UPDATE  {0:s} SET {1:s} WHERE {2:s}=\'{3:s}\''.format(table, add_str, column, str(value))
        #print(query,' ')

        return self.__query(query,'UPDATE')

    def delInfo(self, table, value, column='ID', add_sql=''):
        """

        :param table:
        :param value:
        :param column:
        :param add_sql:
        :return:
        """
        query = 'DELETE FROM %s WHERE `%s`=\'%s\' %s' % (table,column,value,add_sql)
        #print(query, ' ')
        return self.__query(query,'DELETE')

    def truncate(self, table):
        """

        :param table:
        :return:
        """
        query = "TRUNCATE TABLE " + table
        return self.__query(query,'TRUNCATE')

    def __escape(self,text):
        """

        :param text:
        :return:
        """
        text = cgi.escape(text)
        text = text.replace("'","\\'")

        return text

    def getVersion(self):
        """

        :return:
        """
        result = self.getElement("select version()")
        return result

    def begin(self):
        """

        :return: bool true
        """
        try:
            if(self.__dblib=='MySQLdb'):
                self.cnx.begin()
            return True
        except Exception as err:
            # Your exception handling...
            print("Error in fetchassoc:")
            print(err)
            return False


    def rollback(self):
        self.cnx.rollback()

    def commit(self):
        self.cnx.commit()

    def __close(self):
        """

        :return:
        """
        self.cnx.close()

    def __del__(self):
        """

        :return:
        """
        #print('Destructor!')
        self.__close()

