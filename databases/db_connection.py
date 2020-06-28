import psycopg2
import psycopg2.extras
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import pyodbc
from config import app

# server_name='ANTONIO\SQLEXPRESS;'
# db_name='covid_19'
# app = Flask(__name__)
#
# SQLSERVER_CONNECTION_STR="pyodbc://{server}/{db}?driver=SQL Server?Trusted_Connection=yes".format(server=server_name,db=db_name)
# POSTGRES_CONNECTION_STR_DEV = "postgresql://{DB_USER}:{DB_PASS}@{DB_ADDR}:{PORT}/{DB_NAME}".format(DB_USER='postgres', DB_PASS='c', DB_ADDR='localhost',PORT=5432,DB_NAME='postgres')
#
#
# app.config["SQLALCHEMY_ECHO"] = False
# #app.config["SQLALCHEMY_DATABASE_URI"] = SQLSERVER_CONNECTION_STR
# app.config["SQLALCHEMY_DATABASE_URI"] = POSTGRES_CONNECTION_STR_DEV
# app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

db = SQLAlchemy(app)


def get_connection():
    connection = psycopg2.connect(user='postgres',
                                  password='c',
                                  host='localhost',
                                  port=5432,
                                  database='postgres')
    return connection


# def get_connection():
#     conn = pyodbc.connect('Driver={SQL Server};'
#                           'Server=ANTONIO\SQLEXPRESS;'
#                           'Database=covid_19;'
#                           'Trusted_Connection=yes;')
#
#     return conn;

def execute_select_query(select_query: str) -> []:
    """
    - Thực thi lệnh select và trả về mảng các giá trị
    """
    print("select-query: ", select_query)
    connection = get_connection()
    cursor = connection.cursor()
    result = []
    try:
        cursor.execute(select_query)
        cols_description = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        for row in rows:
            data = {}  # dữ liệu của một bản ghi
            for index, value in enumerate(row):
                data[cols_description[index]] = value
            result.append(data)
    except (Exception, psycopg2.Error) as error:
        print("execute_select_query error", error)
        connection.rollback()
        cursor.close()
        connection.close()
        # send_abort(code=400, message="execute_select_query error")
    return result


def select_query(query, connection_temp):
    try:
        connection = connection_temp
        cursor = connection.cursor()
        cursor.execute(query)
        mobile_records = cursor.fetchall()
        lst = []
        for row in mobile_records:
            ls = []
            for r in row:
                ls.append(r)
            lst.append(ls)
        return lst
    except (Exception, pyodbc.Error) as error:
        print("Error while select to SQL Server", error)
        return None
    cursor.close()
    connection.close()
    return mobile_records


def change_data_query(query, connection_temp):
    try:
        connection = connection_temp
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

    except (Exception, pyodbc.Error) as error:
        print("Error while change data Sql Server: ", error)
        print(query)
    finally:
        if (connection):
            cursor.close()
            connection.close()


def get_table_chart_name():
    result=execute_select_query('select distinct table_name from user_tables')
    lst_table=[]
    for x in result:
        lst_table.append(x.get('table_name'))
    result = execute_select_query('select distinct chart_name from chart')
    lst_chart = []
    for x in result:
        lst_chart.append(x.get('chart_name'))

    print('chart: ', lst_chart)
    print('tables: ', lst_table)

    return lst_table,lst_chart

def get_cat_time_value_name(table_name):
    sql_cat = f"select * from user_columns where table_name ='{table_name}'"
    result = execute_select_query(sql_cat)
    lst_cat=[]
    lst_time=[]
    lst_value=[]
    for x in result:
        if x.get('is_category')==1:
            lst_cat.append(x.get('column_name'))
        elif x.get('is_time')==1:
            lst_time.append(x.get('column_name'))
        elif x.get('is_value') == 1:
            lst_value.append(x.get('column_name'))
    print('cat: ',lst_cat)
    print('time: ', lst_time)
    print('value: ', lst_value)
    return lst_cat, lst_time,lst_value

def get_data(table_name,cat_name,time_name,value_name):
    if time_name==None:
        sql_select = f"select {cat_name},sum({value_name}) as {value_name} from {table_name} group by {cat_name}  limit 5"
    elif cat_name==None:
        sql_select = f"select {time_name},sum({value_name}) as {value_name} from {table_name} group by {time_name} limit 5"
    else:
        sql_select=f"select {cat_name},{time_name},sum({value_name}) as {value_name} from {table_name} group by {cat_name},{time_name} limit 5"
    result=execute_select_query(sql_select)

    lst_cat=[]
    lst_time=[]
    lst_value=[]
    for x in result:
        if cat_name!=None:
            lst_cat.append(str(x.get(str(cat_name))))
        if value_name != None:
            lst_value.append(int(x.get(str(value_name))))
        if time_name!=None:
            lst_time.append(x.get(str(time_name)))

    print('category: ', lst_cat)
    print('time: ', len(lst_time))
    print('value: ', lst_value)
    return lst_cat,lst_time,lst_value

def insert_many_geo_distribution(prices, connection_temp):
    sql_str = "insert into geo_distribution_world(active_date,day,month,year,cases,deaths, countries," \
              "geold,country_code,pop_data_2018) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    all_value = [(
        price.active_date,
        price.day,
        price.month,
        price.year,
        price.cases,
        price.deaths,
        price.countries,
        price.geold,
        price.country_code,
        price.pop_data_2018
    ) for price in prices]

    connection = connection_temp
    cursor = connection.cursor()
    try:
        psycopg2.extras.execute_batch(cursor, sql_str, all_value)
        connection.commit()
    except Exception as error:
        print("insert_into_geo_distribution_world::error", error)
        connection.rollback()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    # conn= get_connection()
    # cursor= conn.cursor()
    # cursor.execute('SELECT * FROM partient')
    lst = execute_select_query('SELECT * FROM btl_user')
    #print(lst)
    get_table_chart_name()
    get_cat_time_value_name('geo_distribution_world')
    get_data(table_name='geo_distribution_world',cat_name='countries',time_name=None,value_name='cases')