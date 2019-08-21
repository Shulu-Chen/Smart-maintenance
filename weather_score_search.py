import pandas as pd
import itertools
import warnings
import statsmodels.api as sm
import numpy as np
from datetime import datetime
import csv
from pandas import Series
import yaml
import psycopg2
from sqlalchemy import create_engine

def get_weatherdata(start_date,end_date,config_gw):
    db=config_gw['db_w']
    user_name=config_gw['user']
    pw=config_gw['pswd']
    host_lc=config_gw['host']
    port_lc=config_gw['port']
    conn = psycopg2.connect(database=db, user=user_name,
                            password=pw, host=host_lc, port=port_lc)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM weather.weatherall where date >="+ start_date +' and date <=' + end_date + ' ORDER BY date')
    print('get weather data is successful')
    data = cursor.fetchall()
    data = pd.DataFrame(data)
    data.columns=['id','city','date','wind_level',
                  'status1','status2','hightemp','lowtemp','quality']
    return data

###将结果写入数据库###
def write_weatherdata_to_sql(data,config_w):
    db = config_w['db']
    user_name = config_w['user']
    pw = config_w['pswd']
    host_lc = config_w['host']
    port_lc = config_w['port']
    connect = create_engine(
        'postgresql+psycopg2://' + user_name + ':' + pw + '@' + host_lc + ':' + str(port_lc) + '/' + db)
    pd.io.sql.to_sql(data, 'car_service_city_weather_score', connect, schema='public', if_exists='append')
    connect.dispose()

def count_weather_score(date_start,date_end,config_cw):
    weather_data = get_weatherdata(date_start,date_end,config_cw)
    haze_data = pd.read_csv("C:/Users/Lenovo/Desktop/2画像/weather_haze.csv",encoding='gbk')
    rain_data = pd.read_csv("C:/Users/Lenovo/Desktop/2画像/weather_rain.csv",encoding='gbk')
    snow_data = pd.read_csv("C:/Users/Lenovo/Desktop/2画像/weather_snow.csv",encoding='gbk')
    temp_data = pd.read_csv("C:/Users/Lenovo/Desktop/2画像/weather_temp.csv",encoding='gbk')
    wind_data = pd.read_csv("C:/Users/Lenovo/Desktop/2画像/weather_wind.csv",encoding='gbk')

    weather_data['rain_score1']=10
    weather_data['rain_score2']=10
    weather_data['snow_score1']=10
    weather_data['snow_score2']=10
    weather_data['air_score']=10
    weather_data['wind_score']=10
    weather_data['brktemp_low_score']=10
    weather_data['brktemp_high_score']=10
    weather_data['igttemp_low_score']=10
    weather_data['igttemp_high_score']=10
    weather_data['btytemp_low_score']=10
    weather_data['btytemp_high_score']=10

    for i in range(len(weather_data)):
        id_rain1=rain_data[weather_data.status1[i] == rain_data.weather_type].index.tolist()
        id_rain2=rain_data[weather_data.status2[i] == rain_data.weather_type].index.tolist()
        id_snow1=snow_data[weather_data.status1[i] == snow_data.weather_type].index.tolist()
        id_snow2=snow_data[weather_data.status2[i] == snow_data.weather_type].index.tolist()
        id_wind=wind_data[weather_data.wind_level[i] == wind_data.weather_type].index.tolist()
        id_haze=haze_data[weather_data.quality[i] == haze_data.weather_type].index.tolist()
        id_temp_high=temp_data[weather_data.hightemp[i] == temp_data.temperature].index.tolist()
        id_temp_low=temp_data[weather_data.lowtemp[i] == temp_data.temperature].index.tolist()
        if len(id_rain1) > 0 :
            id=id_rain1[0]
            weather_data.iloc[i,8]=rain_data.iloc[id,1]
        if len(id_rain2) > 0 :
            id=id_rain2[0]
            weather_data.iloc[i,9]=rain_data.iloc[id,1]
        if len(id_snow1) > 0 :
            id=id_snow1[0]
            weather_data.iloc[i,10]=snow_data.iloc[id,1]
        if len(id_snow2) > 0 :
            id=id_snow2[0]
            weather_data.iloc[i,11]=snow_data.iloc[id,1]
        if len(id_wind) > 0 :
            id=id_wind[0]
            weather_data.iloc[i,12]=wind_data.iloc[id,1]
        if len(id_haze) > 0 :
            id=id_haze[0]
            weather_data.iloc[i,13]=haze_data.iloc[id,1]
        if len(id_temp_low) > 0 :
            id=id_temp_low[0]
            weather_data.iloc[i,14] = temp_data.iloc[id,2]
            weather_data.iloc[i,16] = temp_data.iloc[id, 3]
            weather_data.iloc[i,18] = temp_data.iloc[id, 1]
        if len(id_temp_high) > 0 :
            id=id_temp_high[0]
            weather_data.iloc[i,15] = temp_data.iloc[id,2]
            weather_data.iloc[i,17] = temp_data.iloc[id, 3]
            weather_data.iloc[i,19] = temp_data.iloc[id, 1]

    rain_score=(weather_data['rain_score1']+weather_data['rain_score2'])/2
    snow_score=(weather_data['snow_score1']+weather_data['snow_score2'])/2
    brktemp_score=pd.DataFrame((weather_data['brktemp_low_score']+weather_data['brktemp_high_score'])/2)
    btytemp_score=pd.DataFrame((weather_data['btytemp_low_score']+weather_data['btytemp_high_score'])/2)
    igttemp_score=pd.DataFrame((weather_data['igttemp_low_score']+weather_data['igttemp_high_score'])/2)
    rainsnow_score= pd.DataFrame(list(map(lambda x,y:min(x,y), rain_score,snow_score)))
    windhaze_score= pd.DataFrame(list(map(lambda x,y:min(x,y), weather_data['air_score'],weather_data['wind_score'])))

    ##查询字典搜索得分
    rainsnow_a = pd.DataFrame([10,9.5,7.5,7,8.5,6,9,8,6.5,5,4,4.5,3,2,3.5,5.5,2.5,1.5,1])
    rainsnow_b = pd.DataFrame([0.8,0.822222222,0.911111111,0.933333333,0.866666667,0.977777778,0.844444444,
                           0.888888889,0.955555556,1.022222222,1.066666667,1.044444444,1.111111111,
                           1.155555556,1.088888889,1,1.133333333,1.177777778,1.2])
    rainsnow_lib = pd.DataFrame(np.c_[rainsnow_a,rainsnow_b])
    hazewind_a=pd.DataFrame([9,5,8,7,6,1,3,10,4])
    hazewind_b=pd.DataFrame([0.844444444,1.022222222,0.888888889,0.933333333,0.977777778,1.2,1.111111111,0.8,1.066666667])
    hazewind_lib=pd.DataFrame(np.c_[hazewind_a,hazewind_b])
    bty_a = pd.DataFrame([7,8,5.5,6.5,9,10,6,5,4.5,4,3,7.5,3.5,2.5,2,1])
    bty_b = pd.DataFrame([0.933333333,0.888888889,1,0.955555556,0.844444444,0.8,0.977777778,1.022222222,1.044444444,1.066666667,
                      1.111111111,0.911111111,1.088888889,1.133333333,1.155555556,1.2])
    bty_lib = pd.DataFrame(np.c_[bty_a,bty_b])
    brk_a = pd.DataFrame([8.5,7,10,6,5.5,4,4.5])
    brk_b = pd.DataFrame([0.9,1,0.8,1.066666667,1.1,1.2,1.166666667])
    brk_lib  = pd.DataFrame(np.c_[brk_a,brk_b])
    igt_a = pd.DataFrame([8.5,7,10,4,5.5,1])
    igt_b = pd.DataFrame([0.866666667,0.933333333,0.8,1.066666667,1,1.2])
    igt_lib = pd.DataFrame(np.c_[igt_a,igt_b])

    rainsnow_lib.columns = ['rainsnow_score','rainsnow_dis']
    hazewind_lib.columns = ['windhaze_score','windhaze_dis']
    bty_lib.columns = ['btytemp_score','btytemp_dis']
    brk_lib.columns = ['brktemp_score','brktemp_dis']
    igt_lib.columns = ['igttemp_score','igttemp_dis']
    rainsnow_score.columns = ['rainsnow_score']
    windhaze_score.columns = ['windhaze_score']
    btytemp_score.columns= ['btytemp_score']
    brktemp_score.columns = ['brktemp_score']
    igttemp_score.columns = ['igttemp_score']
    rainsnow_data = pd.merge(rainsnow_score, rainsnow_lib, on="rainsnow_score",how='left')
    windhaze_data = pd.merge(windhaze_score, hazewind_lib, on="windhaze_score",how='left')
    btytemp_data=pd.merge(btytemp_score, bty_lib, on="btytemp_score",how='left')
    brktemp_data=pd.merge(brktemp_score, brk_lib, on="brktemp_score",how='left')
    igttemp_data=pd.merge(igttemp_score, igt_lib, on="igttemp_score",how='left')
    #求雨雪、风霾综合得分

    weather_result=np.c_[weather_data['city'],weather_data['date'],rainsnow_data,windhaze_data,btytemp_data,brktemp_data,igttemp_data]
    weather_result=pd.DataFrame(weather_result)
    weather_result.columns=['city','date','rainsnow_score','dis_rainsnow_score','windhaze_score',
                        'dis_windhaze_score','btytemp_score','dis_btytemp_score','brktemp_score',
                        'dis_brktemp_score','igttemp_score','dis_igttemp_score']
    weather_result['visible']='t'
    weather_result['insert_time']=datetime.now()
    print(weather_result['insert_time'])
    weather_result['last_update_time']=datetime.now()
    weather_result['description']='测试'
    data_to_sql = pd.DataFrame(np.c_[weather_result['visible'],weather_result['insert_time'].astype('datetime64'),
                        weather_result['last_update_time'].astype('datetime64'),weather_result['description'],
                        weather_result['city'].astype('str'),weather_result['date'].astype('int'),
                        weather_result['dis_rainsnow_score'].astype('float'),weather_result['dis_windhaze_score'].astype('float'),
                        weather_result['dis_btytemp_score'].astype('float'),weather_result['dis_brktemp_score'].astype('float'),
                        weather_result['dis_igttemp_score'].astype('float')])

    data_to_sql.columns=['visible','insert_time','last_update_time','description','city',
                         'date','dis_rainsnow_score','dis_windhaze_score','dis_btytemp_score',
                         'dis_brktemp_score','dis_igttemp_score']
    data_to_sql['insert_time']=data_to_sql['insert_time'].astype('datetime64')
    data_to_sql['last_update_time']=data_to_sql['last_update_time'].astype('datetime64')
    return data_to_sql

a = datetime.now()
start = "'20180901'"
end = "'20180902'"
config = open('C:/Users/Lenovo/Desktop/PYTHON 售后/aftersale_dev.yaml',encoding='utf-8')
config = yaml.load(config)
data_weather= count_weather_score(start,end,config)
b = datetime.now()
write_weatherdata_to_sql(data_weather,config)
print(b-a,'cost time')