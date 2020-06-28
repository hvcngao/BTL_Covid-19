import pyodbc
import requests
from datetime import  datetime
from bs4 import BeautifulSoup

from databases.db_connection import get_connection, change_data_query
from models.v_covid_global_per_country import GlobalPerCountry


def delete_duplicate(table, column):
    print('Delete Duplicate ')
    query = ''
    try:
        connection = get_connection()
        cursor = connection.cursor()
        query_temp = ''
        for x in column:
            query_temp += str(x) + ', '
        query_temp = query_temp[:len(query_temp) - 2]
        query = '''DELETE FROM ''' + str(table) + ''' WHERE id NOT IN (SELECT MAX(id) FROM ''' + str(
            table) + ''' GROUP BY ''' + query_temp + ''')'''

        cursor.execute(query)
        connection.commit()

    except (Exception, pyodbc.Error) as error:
        print("Error while DeleteDuplicate: ", error)
        if (connection):
            cursor.close()
            connection.close()

def get_covid_global_per_country():
    page = requests.get("https://vi.wikipedia.org/wiki/%C4%90%E1%BA%A1i_d%E1%BB%8Bch_COVID-19_theo_qu%E1%BB%91c_gia_v%C3%A0_v%C3%B9ng_l%C3%A3nh_th%E1%BB%95")
    soup = BeautifulSoup(page.content, 'html.parser')

    temp= soup.find("table", {"class": "wikitable"})
    tbody= temp.find("tbody")
    lst_value=[]
    sql_str = "insert into v_covid_global_per_country(country,confirmed_case,deaths,confirmed_cored) values(N'{}',{},{},{})"
    change_data_query("delete from v_covid_global_per_country", get_connection())
    for tr in tbody.findAll("tr"):
        v_value= GlobalPerCountry()
        tds = tr.findAll("td")
        if(len(tds)>1):
            country= str(tr.find("th").text).replace('\n','').replace('[c]','').strip()
            confirmed_case= float(str(tds[0].text).replace('\n','').replace('.',''))
            deaths = float(str(tds[1].text).replace('\n','').replace('.',''))
            confirmed_cored = float(str(tds[2].text).replace('\n','').replace('.',''))
            lst_value.append(v_value)
            query_temp=sql_str.format(str(country),str(confirmed_case),str(deaths),str(confirmed_cored))
            change_data_query(query_temp,get_connection())

    return lst_value

def get_partient_Viet_Nam_drill_id():
    global id_active_date,id_sex,id_conconfirmed_location,id_hospital,id_nationality,id_note
    id_active_date = id_active_date - 1;
    id_sex = id_sex - 1;
    id_conconfirmed_location = id_conconfirmed_location - 1;
    id_hospital = id_hospital - 1;
    id_nationality = id_nationality - 1;
    id_note = id_note - 1;

def get_partient_Viet_Nam():
    page = requests.get("https://vi.wikipedia.org/wiki/D%C3%B2ng_th%E1%BB%9Di_gian_c%E1%BB%A7a_%C4%91%E1%BA%A1i_d%E1%BB%8Bch_COVID-19_t%E1%BA%A1i_Vi%E1%BB%87t_Nam")
    soup = BeautifulSoup(page.content, 'html.parser')

    temp= soup.findAll("table", {"class": "wikitable"})[1]
    tbody= temp.find("tbody")
    sql_str = "insert into partient(name,active_date,age,sex,confirmed_location,nationality,hospital,had_come_china,had_come_other_country,health_status,note) values({},'{}',{},N'{}',N'{}',N'{}',N'{}',{},{},{},N'{}')"
    change_data_query("delete from partient", get_connection())
   # names=[x.findAll("td")[0].text for x in tbody.findAll("tr")]
    active_dates=[]
    sexs=[]
    confirmed_locations=[]
    nationalitys=[]
    hospitals=[]
    notes=[]


    lst_tr= tbody.findAll("tr")
    for i in range(len(lst_tr)):
        if i==0 or i == len(lst_tr)-1:
            continue
        tds = lst_tr[i].findAll("td")
        if(len(tds)>1):
            id_active_date = 1
            id_sex = 3
            id_conconfirmed_location = 4
            id_nationality = 5
            id_hospital = 6
            id_note = 10


            name=tds[0].text
            #xu ly active_date
            if(len(active_dates)>i-1):
                active_date=active_dates[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                active_date=tds[id_active_date].text
                active_dates.append(active_date)
                if(tds[id_active_date].has_attr("rowspan")):
                    for x in range(int(tds[id_active_date]["rowspan"])-1):
                        active_dates.append( tds[id_active_date].text);
            #age
            age=tds[id_active_date+1].text
            #sex
            if (len(sexs) > i-1):
                sex = sexs[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                sex = tds[id_sex].text
                sexs.append(sex)
                if (tds[id_sex].has_attr("rowspan")):
                    for x in range(int(tds[id_sex]["rowspan"])-1):
                       sexs.append( tds[id_sex].get_text())
            #confirmed_location
            if (len(confirmed_locations) > i-1):
                confirmed_location = confirmed_locations[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                confirmed_location = tds[id_conconfirmed_location].text
                confirmed_locations.append(confirmed_location)
                if (tds[id_conconfirmed_location].has_attr("rowspan")):
                    for x in range(int(tds[id_conconfirmed_location]["rowspan"])-1):
                        confirmed_locations.append( tds[id_conconfirmed_location].get_text())
            #nationality
            if (len(nationalitys) > i-1):
                nationality = nationalitys[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                nationality = tds[id_nationality].text
                nationalitys.append(nationality)
                if (tds[id_nationality].has_attr("rowspan")):
                    for x in range(int(tds[id_nationality]["rowspan"])-1):
                        nationalitys.append(tds[id_nationality].get_text())
            #hospital
            if (len(hospitals) > i-1):
                hospital = hospitals[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                hospital = tds[id_hospital].text
                hospitals.append(hospital)
                if (tds[id_hospital].has_attr("rowspan")):
                    for x in range(int(tds[id_hospital]["rowspan"])-1):
                        hospitals.append(tds[id_hospital].get_text())
            #had_come
            had_come_china=tds[id_hospital+1].text
            had_come_other_country=tds[id_hospital+2].text
            health_status= tds[id_hospital+3].text
            #Note
            if (len(notes) > i-1):
                note = notes[i-1]
                id_active_date = id_active_date - 1;
                id_sex = id_sex - 1;
                id_conconfirmed_location = id_conconfirmed_location - 1;
                id_hospital = id_hospital - 1;
                id_nationality = id_nationality - 1;
                id_note = id_note - 1;
            else:
                note = tds[id_note].text
                notes.append(note)
                if (tds[id_note].has_attr("rowspan")):
                    for x in range(int(tds[id_note]["rowspan"])-1):
                        notes.append( tds[id_note].get_text())

            query_temp=sql_str.format(str(name).replace('\n','')
                                      # ,str(datetime.strptime(str(active_date).replace('\n',''),'%d/%m').date())
                                       , datetime(2020, int(str(active_date).split('/')[1]), int(str(active_date).split('/')[0])).date()

                                        ,0 if str(age).replace('\n','').find('tháng')>-1 else -1 if str(age).replace('\n','').find('?')>-1 else str(age).replace('\n','')
                                      ,str(sex).replace('\n','')
                                      ,str(confirmed_location).replace('\n','')
                                      ,str(nationality).replace('\n','')
                                      ,str(hospital).replace('\n','')
                                      ,1 if str(had_come_china).replace('\n','').find('Có') > -1 else 0
                                      ,1 if str(had_come_other_country).replace('\n','').find('Có') >-1 else 0
                                      ,0 if str(health_status).replace('\n','').find('Đã xuất viện') else 1 if str(health_status).replace('\n','').find('Đang điều trị') else -1
                                      ,str(note).replace('\n','')
                                    );
            print(query_temp)
            change_data_query(query_temp,get_connection())

    return True


if __name__ == "__main__":
    lst= get_partient_Viet_Nam()
    # id_active_date = 1
    # id_sex = 3
    # id_conconfirmed_location = 4
    # id_nationality = 5
    # id_hospital = 6
    # id_note = 10
    # get_partient_Viet_Nam_drill_id()
    # get_partient_Viet_Nam_drill_id()
    # print(id_hospital)
    #get_covid_global_per_country()
