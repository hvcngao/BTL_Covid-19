import os

import requests
import wget
import xlrd
from bs4 import BeautifulSoup
from datetime import  datetime

#from numpy import long
from bson.int64 import long

from databases.db_connection import change_data_query, get_connection, insert_many_geo_distribution
from models.geo_distribution_world import GeoDistributionWorld

file_name=''

def dowload_excel_data():
    url="https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    div= soup.findAll("div", {"class": "media-body"})[3]
    link= div.find("a")
    url_new= link.get('href')

    print('url dowload: ', url_new)
    wget.download(url_new)
    print('dowload done')

def open_file(path):

    global file_name
    file_name=str(path)[(str(path).rfind('/')+1):]
    return xlrd.open_workbook(file_name)

def get_excel_data(sheet):
    change_data_query("delete from geo_distribution_world", get_connection())
    covids=[]
    for i in range(1,sheet.nrows):
        covid=GeoDistributionWorld()
        #active_date=sheet.cell_value(i,0)
        covid.day = int(sheet.cell_value(i,1))
        covid.month= int(sheet.cell_value(i,2))
        covid.year= int(sheet.cell_value(i,3))
        covid.cases= int(sheet.cell_value(i,4))
        covid.deaths= int(sheet.cell_value(i,5))
        covid.countries= sheet.cell_value(i,6)
        covid.geold = sheet.cell_value(i, 7) # vi tri dia ly
        covid.country_code= sheet.cell_value(i,8)
        covid.pop_data_2018= str(sheet.cell_value(i,9))[:str(sheet.cell_value(i,9)).find('.')]   # dan so nam 2018: bo dang sau dau .
        covid.active_date= datetime(covid.year,covid.month,covid.day).date()
        covids.append(covid)

    insert_many_geo_distribution(covids,get_connection())

    os.remove(str(file_name))



if __name__ == "__main__":
    dowload_excel_data()
    get_excel_data(open_file('controllers/COVID-19-geographic-disbtribution-worldwide.xlsx').sheet_by_index(0))