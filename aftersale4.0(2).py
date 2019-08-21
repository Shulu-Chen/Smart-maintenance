# -*- coding: utf-8 -*-
"""
    作者:       Chen Shulu
    版本:       4.0
    日期:       2019/07/29
    项目名称：   Aftersale
    python环境： 3.7
"""
#import cursor as cursor
import pandas as pd
import itertools

from mpmath import plot
from statsmodels.tsa.arima_model import ARIMA
import warnings
import statsmodels.api as sm
import numpy as np
from datetime import datetime
import psycopg2
import sys
import yaml
from sqlalchemy import create_engine
from LogUtils import *

warnings.filterwarnings("ignore")
setup_logging(default_path = "logging.yaml")

def get_obdid(config_id):
    db_lc=config_id['db']
    user_name=config_id['user']
    pw=config_id['pswd']
    host_lc=config_id['host']
    port_lc=config_id['port']
    conn = psycopg2.connect(database=db_lc, user=user_name,
                            password=pw, host=host_lc, port=port_lc)
    cursor = conn.cursor()
    cursor.execute("SELECT distinct (obd_id) FROM wechart_user where obd_id like '817112100153481'")
    data = cursor.fetchall()
    logging.info("smart-service|get_obdid|"+ str(len(data))+"位用户参与此次保养计算")
    return data

###从数据库读车主行驶数据###
def get_customerdata(db_lc,user_name,pw,host_lc,port_lc,obd_id):
    try:
        conn = psycopg2.connect(database=db_lc, user=user_name,
                                password=pw, host=host_lc, port=port_lc)
        cursor = conn.cursor()
        cursor.execute("SELECT id,city,daily_peak_mileages, date_id, mileages, obdid, total_mileages, vehicle_brand"
                       " FROM mileage_daily_records where obdid = " + obd_id + " ORDER BY date_id")
        data = cursor.fetchall()
        data = pd.DataFrame(data)
        data.columns = ['id', 'city', 'daily_peak_mileages', 'driving_date',
                        'daily_mileages', 'obd_id', 'total_mileages', 'vehicle_brand']
        return data
    except Exception as err:
        print(err)


##从数据库读零件数据###
def get_partdata(db_lc,user_name,pw,host_lc,port_lc):
    try:
        conn = psycopg2.connect(database=db_lc, user=user_name,
                                password=pw, host=host_lc, port=port_lc)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM car_parts_service')
        data = cursor.fetchall()
        logging.info("smart-service|get_customerdata|0|用户零部件数据已导入")
        data = pd.DataFrame(data)
        data.columns = ['id','visible','insert_time','last_update','descripation','obd_id',
                        'mtoservise_oil','mtoservise_oil_filter','mtoservise_break_pad','mtoservise_tire',
                         'mtoservise_air_filtration','mtoservise_air_con_filtration','mtoservise_wiper',
                        'mtoservise_spark_plug','mtoservise_battery','mtoservise_antifreezing' ]
        return data
    except Exception as err:
        logging.error("smart-service|get_partdata|1|用户数据获取错误此ID无法计算!!!" )
##从数据库读天气数据###
def get_weascoredata(config_gws):
    try:
        db_lc=config_gws['db']
        user_name=config_gws['user']
        pw=config_gws['pswd']
        host_lc=config_gws['host']
        port_lc=config_gws['port']
        conn = psycopg2.connect(database=db_lc, user=user_name,
                                password=pw, host=host_lc, port=port_lc)
        cursor = conn.cursor()
        cursor.execute('SELECT city,date,dis_rainsnow_score,dis_windhaze_score,'
                       'dis_btytemp_score,dis_brktemp_score,dis_igttemp_score '
                       'FROM car_service_city_weather_score')
        data = cursor.fetchall()
        data = pd.DataFrame(data)
        data.columns = ['city','date','dis_rainsnow_score','dis_windhaze_score','dis_btytemp_score',
                        'dis_brktemp_score','dis_igttemp_score']
        logging.info("smart-service|get_customerdata|0|天气数据已导入")
        return data
    except Exception as err:
        logging.error("smart-service|get_customerdata|1|天气数据获取错误此ID无法计算!!!")

###将结果写入数据库###
def write_data_to_sql(db_w,user_w,pswd_w,host_w,port_w,data):
    connect = create_engine(
        'postgresql+psycopg2://' + user_w + ':' + pswd_w + '@' + host_w + ':' + str(port_w) + '/' + db_w)
    pd.io.sql.to_sql(data, 'car_service_result', connect, schema='public', if_exists='append')
    connect.dispose()
####计算各部件等效里程（天气&拥堵）###
def count_equivalent(data_path,config_path):
    warnings.filterwarnings("ignore")
    traffic_table = pd.read_csv(config_path['get_traffic'])
    weather_table = get_weascoredata(config_path)
    weight_table = pd.read_csv(config_path['get_weight'])

    data = data_path
    data = pd.merge(data, traffic_table, left_on="city",right_on='city_py', how='left')
    data = pd.merge(data, weather_table, left_on=['city_py','driving_date'],
                    right_on=['city','date'], how='left')
    distance_high = data['daily_peak_mileages']
    distance_low = data['daily_mileages']-distance_high
    for i in range(len(distance_low)):
        if distance_low[i] < 0:
            distance_low[i] = 0
    jam_list = distance_high*data['high_score2']+distance_low*data['low_score2']
    data = data.replace(float('NaN'),1)
    rainsnow_list = data['daily_mileages'] * data['dis_rainsnow_score']
    windhaze_list = data['daily_mileages']*data['dis_windhaze_score']
    bty_list = data['daily_mileages']*data['dis_btytemp_score']
    brk_list = data['daily_mileages']*data['dis_brktemp_score']
    igt_list = data['daily_mileages']*data['dis_igttemp_score']
    mile_df = pd.concat([rainsnow_list,windhaze_list,brk_list,
                         bty_list,igt_list,jam_list],axis =1 )
    part_list = []
    breaks = 0
    tire = 0
    airfilter= 0
    airconfilter= 0
    wiper= 0
    ignite= 0
    battery= 0
    antifreeze= 0
    oil= 0
    oil_filter= 0
    for i in range(6):
        breaks = weight_table.ix[0,i+1]*mile_df[i]+breaks
        tire= weight_table.ix[1,i+1]*mile_df[i]+tire
        airfilter= weight_table.ix[2,i+1]*mile_df[i]+airfilter
        airconfilter= weight_table.ix[3,i+1]*mile_df[i]+airconfilter
        wiper= weight_table.ix[4,i+1]*mile_df[i]+wiper
        ignite= weight_table.ix[5,i+1]*mile_df[i]+ignite
        battery= weight_table.ix[7,i+1]*mile_df[i]+battery
        antifreeze= weight_table.ix[8,i+1]*mile_df[i]+antifreeze
        oil= weight_table.ix[9,i+1]*mile_df[i]+oil
        oil_filter= weight_table.ix[10,i+1]*mile_df[i]+oil_filter
    breaks = breaks/weight_table.ix[0,7]
    tire = tire/weight_table.ix[1,7]
    airfilter= airfilter/weight_table.ix[2,7]
    print(airfilter)
    print('------------------------------------------------------------------------')
    airconfilter= airconfilter/weight_table.ix[3,7]
    wiper= wiper/weight_table.ix[4,7]
    ignite= ignite/weight_table.ix[5,7]
    battery= battery/weight_table.ix[7,7]
    antifreeze= antifreeze/weight_table.ix[8,7]
    oil= oil/weight_table.ix[9,7]
    oil_filter=oil_filter/weight_table.ix[10,7]
    mile_equivalent = pd.concat([breaks,tire,airfilter,airconfilter,
                         wiper,ignite,battery,antifreeze,oil,oil_filter],axis =1 )
    mile_equivalent.columns= ['breaks','tire','airfilter','airconfilter',
                         'wiper','ignite','battery','antifreeze','oil','oil_filter']
    mile_equivalent.loc['Col_sum'] = mile_equivalent.apply(lambda x: x.sum())
    mile_sum = mile_equivalent.ix[-1,]
    return mile_sum
###判断函数###
def exam (part,part_mile,start_mile):
  for i in range(len(part_mile)):
    if (part_mile.mile[len(part_mile)-1] < start_mile):
      print("您无需保养",part)
      day = 90
      break
    elif (part_mile.mile[0] >= start_mile):
      print("您急需保养", part)
      day = 0
      break
    elif (part_mile.mile[i] >= start_mile):
      if (i >= 0 and i <= 30):
        print("您需要在", i+1, "天内对", part, "进行保养")
        day = i+1
        break
      elif (i > 30):
        print("您无需保养",part)
        day = i
        break
    else :
     pass
  return day
###接入车主行驶里程时的剩余保养里程###
def count_leftmile(car_type,total_mile,last_maintain,config_lf):
  last_miles = []
  psa_mile = config_lf['psa_miles']
  kia_mile = config_lf['kia_miles']
  if (car_type == 'PSA'):#psa
    fixmile = psa_mile
    for i in range(10):
      if (last_maintain[i] > 0):   #上次保养时的总里程（有数）
        last_miles.append(fixmile[i]-total_mile + last_maintain[i])
      else:                        #上次保养时的总里程（没数）
        last_miles.append(fixmile[i]-total_mile%fixmile[i])
  elif (car_type == 'KIA'):
    fixmile = kia_mile
    for i in range(10):
      if (last_maintain[i] > 0):
        last_miles.append(fixmile[i]-total_mile + last_maintain[i])
      else:
        last_miles.append(fixmile[i] - total_mile % fixmile[i])
  return last_miles
###计算百分比###
def count_percent(car_type,last_mile,equal_miles,part_status,total_miles,config_cp):
    psa_mile = config_cp['psa_miles']
    kia_mile = config_cp['kia_miles']
    perc = []
    if (car_type == 'PSA'):  # psa
        fixmile = psa_mile
        for i in range(10):
            if (total_miles>=part_status[i]):
                perc.append(1-(last_mile[i]-equal_miles[i])/fixmile[i])
            else:
                perc.append((equal_miles[i]+total_miles-part_status[i])/fixmile[i])
    elif (car_type == 'KIA'):
        fixmile = kia_mile
        for i in range(10):
            if (total_miles>=part_status[i]):
                perc.append(1-(last_mile[i]-equal_miles[i])/fixmile[i])
            else:
                perc.append((equal_miles[i]+total_miles-part_status[i])/fixmile[i])
    for j in range(10):
        if (perc[j]>1):
            perc[j]=1
        elif (perc[j]<0):
            perc[j]=0
    perc = pd.DataFrame(perc)
    perc = perc.T
    perc.columns=['刹车片','轮胎','空气滤清器','空调滤清器','雨刮器','火花塞','电池系统','防冻液','机油','机油滤清器']
    return perc
###主函数###
def smart_aftersale(mileage,partstatus,config_yaml):
      warnings.filterwarnings("ignore")
      part_name = ['mtoservise_oil','mtoservise_oil_filter','mtoservise_break_pad','mtoservise_tire',
                   'mtoservise_air_filtration','mtoservise_air_con_filtration','mtoservise_wiper',
                   'mtoservise_spark_plug','mtoservise_battery','mtoservise_antifreezing']
      try:
        mile_data = mileage
        city = mile_data['city'][1]
        totalmiles = mileage['total_mileages'][1]
        #totalmiles = 10000000
        if (totalmiles <= 0):
            logging.error("smart-service|smart_aftersale|1|totalmiles <= 0,模型计算失败，此ID无法计算!!!")
        carbrand = mileage['vehicle_brand'][1]
        print(carbrand)
        customer_id = mileage['obd_id'][1]
        part_id=partstatus[partstatus.obd_id == customer_id].index.tolist()
        if (len(part_id)>0):
            part_id = part_id[0]
            part_mileage = []
            for i in part_name:
                part_mileage.append(partstatus[i][part_id])
        else:
            part_mileage = [-1,-1,-1,-1,-1,-1,-1,-1,-1,-1]
        print(part_mileage,'part_mileage')
        pre_day = 90 #预测天数
        start_mileage = count_leftmile(carbrand,totalmiles,part_mileage,config_yaml)
        oil_st = start_mileage[0]
        oilfilter_st = start_mileage[1]
        breaks_st = start_mileage[2]
        tire_st = start_mileage[3]
        airfilter_st = start_mileage[4]
        airconfilter_st = start_mileage[5]
        wiper_st = start_mileage[6]
        ignition_st = start_mileage[7]
        battery_st = start_mileage[8]
        antifrezze_st = start_mileage[9]
        ## 数据清洗,在数据缺失处添加日期，里程数取0##
        calendar = pd.read_csv(config_yaml['get_calendar'],encoding = 'gbk')
        calendar.columns= ['date_id']

        t1 = mile_data['driving_date'][0]
        t2 = mile_data['driving_date'][len(mile_data)-1]
        t3 = calendar[calendar.date_id==t1].index.tolist()
        t4 = calendar[calendar.date_id==t2].index.tolist()
        t3 = int(t3[0])
        t4 = int(t4[0])
        date = calendar.date_id[t3:t4+1]
        mile_data = pd.merge(date, mile_data, left_on='date_id',right_on='driving_date' ,how='left')
        mile_data = mile_data.fillna(0)
        ###进行综合评分###
        mile_equivalent = count_equivalent(mile_data,config_yaml)
        percent_part = count_percent(carbrand,start_mileage,mile_equivalent,part_mileage,totalmiles,config_yaml)#计算各部件保养百分比
        left_miles=start_mileage-mile_equivalent
        breaksmiled = mile_equivalent[0]
        print(breaksmiled.astype(int))
        print('-------------------------------------------------------------------------------------------')
        tiremiled = mile_equivalent[1]
        airfiltermiled = mile_equivalent[2]
        airconfiltermiled = mile_equivalent[3]
        wipermiled = mile_equivalent[4]
        ignitionmiled = mile_equivalent[5]
        batterymiled = mile_equivalent[6]
        antifrezzemiled = mile_equivalent[7]
        oilmiled = mile_equivalent[8]
        oilfiltermiled = mile_equivalent[9]
        ##Arima时序模型预测
        warnings.filterwarnings("ignore")  # specify to ignore warning messages
        #处理数据
        start = str(t1)
        start_pre = str(t2) #起始预测日期（读取里程的最后一日）
        end_pre = str(calendar.date_id[t4+pre_day])
        per = len(mile_data)
        s = mile_data['daily_mileages'].values.tolist()
        train_data = pd.DataFrame(s, columns=['mile'],
                                index=pd.date_range(start=start,periods=per)) #为时序模型构造日期序列
        train_data['mile'] = train_data['mile'].astype('float64')
        ##Arima模型###
        #arima = ARIMA(train_data, order=(7,1,2))
        #model = arima.fit(disp=False)
        #print(model.aic, model.bic, model.hqic)
        ##Seasonal Arima模型
        d = range(1, 2)
        q = range(5, 6)
        p = range(5, 8)
        # Generate all different combinations of p, q and q triplets
        pdq = list(itertools.product(p, d, q))
        # Generate all different combinations of seasonal p, q and q triplets
        seasonal_pdq = [(1,0,0,52)]
        params = []
        params_seasonal = []
        aics = []
        for param in pdq:
          for param_seasonal in seasonal_pdq:
            try:
              mod = sm.tsa.statespace.SARIMAX(train_data,
                                              order=param,
                                              seasonal_order=param_seasonal,
                                              enforce_stationarity=False,
                                              enforce_invertibility=False)
              results = mod.fit(disp=False).aic

              params.append(param)
              params_seasonal.append(param_seasonal)
              aics.append(results)
              print('ARIMA{}x{} - AIC:{}'.format(param, param_seasonal, results))
            except:
              continue
        num = aics.index(min(aics))
        param = params[num]
        param_seasonal = params_seasonal[num]
        mod = sm.tsa.statespace.SARIMAX(train_data,
                                        order=param,
                                        seasonal_order=param_seasonal,
                                        enforce_stationarity=False,
                                        enforce_invertibility=False)
        results = mod.fit()


        #预测
        pred = results.predict(start=start_pre, end=end_pre, dynamic= True)
        for i in range(len(pred)):
          if pred[i] < 0:
            pred[i] = 0
        pred.index = range(len(pred))
        pred = pd.DataFrame(pred)
        pred.columns= ['mile']
        total_miles_brk = breaksmiled + pred.cumsum()
        print(pred.cumsum()/1000000)
        print('-------------------------------------------------------------------------------------------')
        total_miles_tie = tiremiled + pred.cumsum()
        total_miles_aft = airfiltermiled + pred.cumsum()
        total_miles_acft = airconfiltermiled + pred.cumsum()
        total_miles_wpr = wipermiled + pred.cumsum()
        total_miles_igt = ignitionmiled + pred.cumsum()
        total_miles_bty = batterymiled + pred.cumsum()
        total_miles_atf = antifrezzemiled + pred.cumsum()
        total_miles_oil = oilmiled + pred.cumsum()
        total_miles_oif = oilfiltermiled + pred.cumsum()
        days = []
        days.append(exam('刹车片',total_miles_brk,breaks_st))
        days.append(exam('轮胎',total_miles_tie,tire_st))
        days.append(exam('空气滤清器',total_miles_aft,airfilter_st))
        days.append(exam('空调滤清器',total_miles_acft,airconfilter_st))
        days.append(exam('雨刮器',total_miles_wpr,wiper_st))
        days.append(exam('火花塞',total_miles_igt,ignition_st))
        days.append(exam('电池',total_miles_bty,battery_st))
        days.append(exam('防冻液',total_miles_atf,antifrezze_st))
        days.append(exam('机油',total_miles_oil,oil_st))
        days.append(exam('机油滤清器',total_miles_oif,oilfilter_st))
        fixdays = pd.DataFrame(days)
        fixdays = fixdays.T
        fixdays.columns = ['刹车片','轮胎','空气滤清器','空调滤清器','雨刮器','火花塞',
                           '电池系统','防冻液','机油','机油滤清器']
        data_to_sql = ['',str(datetime.now()),str(datetime.now()),'t',int(fixdays['空调滤清器'][0]),
                       int(fixdays['空气滤清器'][0]),int(fixdays['防冻液'][0]),int(fixdays['电池系统'][0]),
                       int(fixdays['刹车片'][0]),
                       int(fixdays['机油'][0]),int(fixdays['机油滤清器'][0]),int(fixdays['火花塞'][0]),
                       int(fixdays['轮胎'][0]),
                       int(fixdays['雨刮器'][0]),float(left_miles['airconfilter']),float(left_miles['airfilter']),
                       float(left_miles['antifreeze']),float(left_miles['battery']),float(left_miles['breaks']),
                       float(left_miles['oil']),float(left_miles['oil_filter']),float(left_miles['ignite']),
                       float(left_miles['tire']),float(left_miles['wiper']),str(customer_id),float(percent_part['空调滤清器'][0]),
                       float(percent_part['空气滤清器'][0]),float(percent_part['防冻液'][0]),float(percent_part['电池系统'][0]),
                       float(percent_part['刹车片'][0]),float(percent_part['机油'][0]),float(percent_part['机油滤清器'][0]),
                       float(percent_part['火花塞'][0]),float(percent_part['轮胎'][0]),
                       float(percent_part['雨刮器'][0]),start_pre]
        data_to_sql = pd.DataFrame(data_to_sql)
        data_to_sql =data_to_sql.T
        data_to_sql.columns=['description','insert_time','last_update_time','visible',
                             'dtoservise_air_con_filtration','dtoservise_air_filtration',
                             'dtoservise_antifreezing','dtoservise_battery','dtoservise_break_pad',
                             'dtoservise_oil','dtoservise_oil_filter','dtoservise_spark_plug',
                             'dtoservise_tire','dtoservise_wiper','mtoservise_air_con_filtration',
                             'mtoservise_air_filtration','mtoservise_antifreezing','mtoservise_battery',
                             'mtoservise_break_pad','mtoservise_oil','mtoservise_oil_filter',
                             'mtoservise_spark_plug','mtoservise_tire','mtoservise_wiper','obd_id',
                             'ptoservise_air_con_filtration','ptoservise_antifreezing','ptoservise_battery',
                             'ptoservise_break_pad','ptoservise_oil','ptoservise_oil_filter',
                             'ptoservise_spark_plug','ptoservise_tire','ptoservise_wiper',
                             'ptoservise_air_filtration','date_id']
        logging.info("smart-service|smart_aftersale|0|模型计算已完成")
        return data_to_sql
      except Exception as err:
        logging.error("smart-service|smart_aftersale|1|模型计算失败此ID无法计算!!!" )


def get_index(db,user_name,pw,host_lc,port_lc):
    conn = psycopg2.connect(database=db, user=user_name,
                            password=pw, host=host_lc, port=port_lc)
    cursor = conn.cursor()
    cursor.execute('select max(index) from car_service_result')
    max_index = cursor.fetchall()
    return max_index

def new_result_data(obd_id):
    try:
        config = open('./aftersale_dev.yaml',encoding='utf-8')
        config = yaml.load(config)
        db = config['db']
        user = config['user']
        pswd = config['pswd']
        host = config['host']
        port = config['port']
        miles_list = get_customerdata(db, user, pswd, host, port,obd_id)
        part_status = get_partdata(db, user, pswd, host, port)
        result_data = smart_aftersale(miles_list,part_status,config)
        to_sql_columns = ['description', 'insert_time', 'last_update_time', 'visible',
                               'dtoservise_air_con_filtration', 'dtoservise_air_filtration',
                               'dtoservise_antifreezing', 'dtoservise_battery', 'dtoservise_break_pad',
                               'dtoservise_oil', 'dtoservise_oil_filter', 'dtoservise_spark_plug',
                               'dtoservise_tire', 'dtoservise_wiper', 'mtoservise_air_con_filtration',
                               'mtoservise_air_filtration', 'mtoservise_antifreezing', 'mtoservise_battery',
                               'mtoservise_break_pad', 'mtoservise_oil', 'mtoservise_oil_filter',
                               'mtoservise_spark_plug', 'mtoservise_tire', 'mtoservise_wiper', 'obd_id',
                               'ptoservise_air_con_filtration', 'ptoservise_antifreezing', 'ptoservise_battery',
                               'ptoservise_break_pad', 'ptoservise_oil', 'ptoservise_oil_filter',
                               'ptoservise_spark_plug', 'ptoservise_tire', 'ptoservise_wiper',
                               'ptoservise_air_filtration', 'date_id']
        max_index = get_index(config['db'], config['user'], config['pswd'], config['host'], config['port'])
        data_result_new = pd.DataFrame(result_data.values, columns=to_sql_columns,
                                    index = [max_index[0][0] + 1])
        logging.info("smart-service|new_result_data|0|保养数据已完成")
        return data_result_new
    except Exception as err:
        logging.error("smart-service|new_result_data|1|保养数据获取错误此ID无法计算!!!")


def run(obd_id):
    try:
        config = open('./aftersale_dev.yaml', encoding='utf-8')
        config = yaml.load(config)
        db = config['db']
        user = config['user']
        pswd = config['pswd']
        host = config['host']
        port = config['port']
        a = datetime.now()
        result_data = new_result_data(obd_id)
        write_data_to_sql(db,user,pswd,host,port,result_data)
        b = datetime.now()
        print('total run time:', b - a)
        logging.info("smart-service|run|0|售后程序已完成")
    except Exception as err:
        logging.error("smart-service|run|1|售后程序错误此ID无法计算!!!")
        logging.info("--------------------------------------------------------------------")


def loop():
    config = open('./aftersale_dev.yaml',encoding='utf-8')
    config = yaml.load(config)
    obd_list = get_obdid(config)
    print(obd_list)
    config = open('./aftersale_dev.yaml', encoding='utf-8')
    config = yaml.load(config)
    for obd_id in obd_list:
        db = config['db']
        user = config['user']
        pswd = config['pswd']
        host = config['host']
        port = config['port']
        miles_list = get_customerdata(db, user, pswd, host, port, "'"+obd_id[0]+"'")
        try:
            if (len(list(miles_list['total_mileages'])) != 0) and (miles_list['total_mileages'][0] is not None):
                if miles_list['total_mileages'][0]<=0:
                    logging.error("smart-service|get_customerdata|1|" + obd_id[0] + "用户数据获取错误此ID无法计算!!!")
                    continue
                else:
                    logging.info("smart-service|get_customerdata|0|" + obd_id[0] + "用户数据已导入")
                    run("'"+obd_id[0]+"'")
            else:
                logging.error("smart-service|get_customerdata|1|" +  obd_id[0] + "用户数据获取错误此ID无法计算!!!")
                continue
        except Exception as err:
            logging.error("smart-service|get_customerdata|1|" +  obd_id[0] + "用户数据获取错误此ID无法计算!!!")


if __name__ == "__main__":
    loop()


