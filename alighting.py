#coding=utf-8
import sqlite3
import os,time
import csv
import math
from collections import defaultdict

'''SQLite数据库操作的类'''
class SqliteDB:
	def __init__(self,path):
		self.path = path
		
	def get_conn(self):
		#获取数据库链接对象，参数为数据库文件的绝对路径，如果参数不存在，返回内存的数据链接对象
		conn = sqlite3.connect(self.path)
		if os.path.exists(self.path) and os.path.isfile(self.path):
			return conn
		else:
			conn = None
			return sqlite3.connect(':memory:')

	def get_cursor(self,conn):
		#获取数据库游标对象
		if conn is not None:
			return conn.cursor()
		else:
			return self.get_conn('').cursor()

	def drop_table(self,conn,table):
		#如果表存在，则删除表
		if table is not None and table != '':
			sql = 'DROP TABLE IF EXISTS ' + table
			cu = self.get_cursor(conn)
			cu.execute(sql)
			conn.commit()
	
	def create_table(self,conn,sql):
		#创建表
		if sql is not None and sql!= '':
			cu = self.get_cursor(conn)
			cu.execute(sql)
			conn.commit()
			#self.close_all(conn,cu)

	def close_all(self,conn,cu):
		#关闭数据库游标对象和数据库链接对象
		try:
			if cu is not None:
				cu.close()
		finally:
			if conn is not None:
				conn.close()
				
	def save(self,conn,sql,data):
		#插入数据
		if sql is not None and sql != '':
			if data is not None:
				cu = self.get_cursor(conn)
				for d in data:
					cu.execute(sql,d)
					#conn.commit()
				#self.close_all(conn,cu)
		
	def fet_chall(self,conn,sql):
		#查询所有数据
		if sql is not None and sql != '':
			cu = self.get_cursor(conn)
			cu.execute(sql)
			r = cu.fetchall()
		
	def fetch_one(self,conn,sql,data):
		#查询一条数据
		if sql is not None and sql != '':
			if data is not None:
				d = (data,)
				cu = self.get_cursor(conn)
				cu.execute(sql,d)
				r = cu.fetchall()
				if len(r) > 0:
					for e in range(len(r)):
						print(r[e])
		
	def update(self,conn,sql,data):
		#更新数据
		if sql is not None and sql != '':
			if data is not None:
				cu = self.get_cursor(conn)
				for d in data:
					cu.execute(sql,d)
					#conn.commit()
				#self.close_all(conn,cu)
	
	def delete(self,conn,sql,data):
		#删除数据
		if sql is not None and sql != '':
			if data is not None:
				cu = self.get_cursor(conn)
				for d in data:
					cu.execute(sql,d)

	
#获取所有的route_id
def get_routeId():
	routeids = []
	getRouteID_sql = 'SELECT route_id FROM BUS_ROUTE_DIC GROUP BY route_id'
	cu.execute(getRouteID_sql)
	r = cu.fetchall()
	for i in range(len(r)):
		routeids.append(r[i][0])
	return routeids
	
#获取同一route_id的所有bus_id
def get_busId(routeid):
	busids = []
	getBusID_sql = 'SELECT bus_id FROM BUS_ROUTE_DIC WHERE route_id = {}'.format(routeid)
	cu.execute(getBusID_sql)
	r = cu.fetchall()
	for i in range(len(r)):
		busids.append(r[i][0])
	return busids
	
#获取相同route_id和direction的GPS点
def getPoints(busids,direction):
	all_points=[]
	for _id in busids:
		getGPS_sql = 'SELECT lng, lat FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND direction = {}'.format(_id,direction)
		cu.execute(getGPS_sql)
		data = cu.fetchall()
		all_points.extend(data)
	return all_points
	
#将GPS的WGS84坐标（大地坐标系）转换为平面坐标（笛卡尔坐标系）的方法
def GeodeticToCartesian(geoCoord):
	'''L0:中央经线度数
	N: double; // 卯酉圈曲率半径
	X: double; // 高斯平面纵坐标
	Y: double; // 高斯平面横坐标
	s: double; // 赤道至纬度B的经线弧长
	f: double; // 参考椭球体扁率
	e2: double; // 椭球第一偏心率
	a: double; // 参考椭球体长半轴'''
	loc = []
	Datum = 84 #投影基准面类型：北京54基准面为54，西安80基准面为80，WGS84基准面为84
	prjno = 0 #投影带号
	zonewide = 3
	IPI = 0.0174532925199433333333 # 3.1415926535898/180.0
	b = geoCoord[1] #纬度
	L = geoCoord[0] #经度
	if zonewide==6:
		prjno = int(L / zonewide) + 1
		L0 = prjno * zonewide - 3 
	else:
		prjno = int((L-1.5)/3) + 1
		L0 = prjno * 3
	if Datum==54:
		a = 6378245
		f = 1 / 298.3
	elif Datum==84:
		a = 6378137
		f = 1 / 298.257223563
	L0 = L0 * IPI
	L = L * IPI
	b = b * IPI
	e2 = 2 * f - f * f #(a*a-b*b)/(a*a)
	L1 = L - L0
	t = math.tan(b)
	m = L1 * math.cos(b)
	N = a / math.sqrt(1 - e2 * math.sin(b) * math.sin(b))
	q2 = e2 / (1 - e2) * math.cos(b) * math.cos(b)
	a1 = 1 + 3 / 4 * e2 + 45 / 64 * e2 * e2 + 175 / 256 * e2 * e2 * e2 + 11025 / 16384 * e2 * e2 * e2 * e2 + 43659 / 65536 * e2 * e2 * e2 * e2 * e2
	a2 = 3 / 4 * e2 + 15 / 16 * e2 * e2 + 525 / 512 * e2 * e2 * e2 + 2205 / 2048 * e2 * e2 * e2 * e2 + 72765 / 65536 * e2 * e2 * e2 * e2 * e2
	a3 = 15 / 64 * e2 * e2 + 105 / 256 * e2 * e2 * e2 + 2205 / 4096 * e2 * e2 * e2 * e2 + 10359 / 16384 * e2 * e2 * e2 * e2 * e2
	a4 = 35 / 512 * e2 * e2 * e2 + 315 / 2048 * e2 * e2 * e2 * e2 + 31185 / 13072 * e2 * e2 * e2 * e2 * e2
	b1 = a1 * a * (1 - e2)
	b2 = -1 / 2 * a2 * a * (1 - e2)
	b3 = 1 / 4 * a3 * a * (1 - e2)
	b4 = -1 / 6 * a4 * a * (1 - e2)
	c0 = b1
	c1 = 2 * b2 + 4 * b3 + 6 * b4
	c2 = -(8 * b3 + 32 * b4)
	c3 = 32 * b4
	s = c0 * b + math.cos(b) * (c1 * math.sin(b) + c2 * math.sin(b) * math.sin(b) * math.sin(b) + c3 * math.sin(b) * math.sin(b) * math.sin(b) * math.sin(b) * math.sin(b))
	X = s + 1 / 2 * N * t * m * m + 1 / 24 * (5 - t * t + 9 * q2 + 4 * q2 * q2) * N * t * m * m * m * m + 1 / 720 * (61 - 58 * t * t + t * t * t * t) * N * t * m * m * m * m * m * m
	Y = N * m + 1 / 6 * (1 - t * t + q2) * N * m * m * m + 1 / 120 * (5 - 18 * t * t + t * t * t * t - 14 * q2 - 58 * q2 * t * t) * N * m * m * m * m * m
	Y = Y + 1000000 * prjno + 500000
	loc.append(X)
	loc.append(Y - 38000000)
	loc.append(0)
	return loc

#投影后计算平面距离的函数，单位是米
def dist2(p1,p2):
	if p1[0] == p2[0] and p1[1] == p2[1]:
		return 0.0
	else:
		x_y1 = GeodeticToCartesian(p1)
		x_y2 = GeodeticToCartesian(p2)
		dis = math.sqrt((x_y1[0]-x_y2[0])*(x_y1[0]-x_y2[0]) + (x_y1[1]-x_y2[1])*(x_y1[1]-x_y2[1]))
		return dis
	
#计算afc刷卡数据中，每一个bus_id对应的day
def busId_day(bus_id):
	days = []
	day_sql = 'SELECT day FROM GPS_DATA_CLASSIFY WHERE bus_id = {} GROUP BY day'.format(bus_id)
	cu.execute(day_sql)
	r = cu.fetchall()
	if len(r) > 0:
		for i in range(len(r)):
			days.append(r[i][0])
	return days
	
'''第二部分：下客点的计算。步骤如下：
1.将AFC数据存入数据库表；
2.对AFC数据按照时间进行聚类，求出同一站上车的第一个刷卡时间为此站所有乘客的上车时间，将结果存入数据库表. —— afc_time(busid,day)；
3.根据AFC聚类后的上车时间和GPS数据，求出每一个时间对应的坐标，根据坐标在第一部分的结果里面求对应的站点，存储至表. —— save_BusStop()；
4.对照2，3中的两个表，根据每个guid上车的时间、所乘车辆，求出对应的上车站点信息(行进方向，坐标，第几站). —— save_AfcRide()；
5.对同一card_id的数据进行分类，分为 a.多次刷卡记录(a1.一天有多次刷卡记录，a2.一天只有单次刷卡记录)、b.单次刷卡记录3种类型，
分别根据出行链、card_id最热上车站、乘车下游最热上车站的方法来计算下车点. —— save_Alighting()；
6.将计算下客点计算结果输出。'''

#创建数据表AFC_DATA_ABOARD,存储每个人对应的准确的上车时间,`aboard_time`为同一站点上车的第一条刷卡记录的时间
def create_AboardTable():
	create_table_sql = '''CREATE TABLE `AFC_DATA_ABOARD`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`guid` int(11) NOT NULL,
						`card_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`aboard_time` int(11) NOT NULL,
						`bus_id` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)
	
#创建数据表BUS_STOP,存储each bus_id each day 的到站时间和坐标，`sequence`是站点的次序，`count`为此站上车的人数
def create_BusStopTable():
	create_table_sql = '''CREATE TABLE `BUS_STOP`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`bus_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`time` int(11) NOT NULL,
						`direction` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL,
						`sequence` int(11) NOT NULL,
						`count` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)
	
#创建数据表AFC_DATA_RIDE，存车每个人对应的上车点
def create_RideTable():
	create_table_sql = '''CREATE TABLE `AFC_DATA_RIDE`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`guid` int(11) NOT NULL,
						`card_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`aboard_time` int(11) NOT NULL,
						`bus_id` int(11) NOT NULL,
						`direction` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL,
						`sequence` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)

#each bus_id day 的刷卡数据，求出每一天停靠站点的时间,和同一站点上车的人数
def afc_time(busid,day):
	getAfc_sql = 'SELECT * FROM AFC_DATA WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(busid,day)
	cu.execute(getAfc_sql)
	r = cu.fetchall()
	stopTimes = []
	upNums = []
	time0 = r[0][4]
	for j in range(len(r)):
		if j == 0:
			stopTimes.append(time0)
			upNums.append(j)
			save_sql = 'INSERT INTO AFC_DATA_ABOARD values (?, ?, ?, ?, ?, ?)'#存储每一个guid的上车时间至AFC_DATA_ABOARD
			data = [(None,r[j][1],r[j][2],r[j][3],time0,r[j][5])]
			db.save(conn,save_sql,data)
			continue
		'''认为与上次刷卡时间差超过20s，且与上一站第一次刷卡时间差超过90s的刷卡记录为下一站的第一次刷卡记录.
		这两个时间间隔是关键参数，如果修改会导致下客点的计算结果的变化。此参数目前没有调至最优，继续减小时间间隔可能会提高正确率。'''
		if r[j][4] - r[j-1][4] > 25 and r[j][4] - time0 > 90:
			time0 = r[j][4]
			stopTimes.append(time0)
			upNums.append(j)
		save_sql = 'INSERT INTO AFC_DATA_ABOARD values (?, ?, ?, ?, ?, ?)'
		data = [(None,r[j][1],r[j][2],r[j][3],time0,r[j][5])]
		db.save(conn,save_sql,data)
	upNums.append(len(r))
	result = []#存储同一站点上车的人数
	for k in range(len(upNums)):
		if k == 0:
			continue
		result.append([stopTimes[k-1],upNums[k]-upNums[k-1]])
	conn.commit()
	return result
	
#求刷卡数据的所有的bus_ids
def afcBusId():
	afc_busId = []
	sel_sql = 'SELECT bus_id FROM AFC_DATA GROUP BY bus_id'
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 0:
		for i in range(len(r)):
			afc_busId.append(r[i][0])
	return afc_busId
	
#求刷卡数据的所有的card_id
def afcCardId():
	afc_cardId = []
	sel_sql = 'SELECT card_id FROM AFC_DATA GROUP BY card_id'
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 0:
		for i in range(len(r)):
			afc_cardId.append(r[i][0])
	return afc_cardId
	
#计算afc刷卡数据中，每一个bus_id对应的day
def afc_day(bus_id):
	days = []
	day_sql = 'SELECT day FROM AFC_DATA WHERE bus_id = {} GROUP BY day'.format(bus_id)
	cu.execute(day_sql)
	r = cu.fetchall()
	if len(r) > 0:
		for i in range(len(r)):
			days.append(r[i][0])
	return days

#根据bus_id,day,time找上车的坐标，查找时间间隔最小的点的坐标，若没有，返回[0.0,0.0]
def afcLoc(bus_id,day,time):
	sel_sql = 'SELECT * FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND day = {} AND time <= {} ORDER BY time ASC'.format(bus_id,day,time)
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 0:
		lng = r[-1][4]
		lat = r[-1][5]
		direction = r[-1][6]
		return [direction,[lng,lat]]
	else:
		sel_sql = 'SELECT * FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND day = {} AND time >= {} ORDER BY time ASC'.format(bus_id,day,time)
		cu.execute(sel_sql)
		r2 = cu.fetchall()
		if len(r2) > 0:
			lng = r2[0][4]
			lat = r2[0][5]
			direction = r2[0][6]
			return [direction,[lng,lat]]
		else:
			return [0,[0.0,0.0]]
	
#根据bus_id,lng,lat求其所属route_id的对应的站点的坐标和序号
def afcStop(bus_id,direction,pt):
	sel_sql = 'SELECT route_id FROM BUS_ROUTE_DIC WHERE bus_id = {}'.format(bus_id)
	cu.execute(sel_sql)
	r = cu.fetchall()
	route_id = r[0][0]
	sel_sql = 'SELECT sequence,lng,lat FROM RESULT_STOP_LIST WHERE route_id = {} AND direction = {}'.format(route_id,direction)
	cu.execute(sel_sql)
	r = cu.fetchall()
	#查找不到结果，返回sequence = 1, 坐标为pt
	if len(r) == 0:
		return [1,pt[0],pt[1]]
	minDist = 3000.0
	stop = [r[0][0],r[0][1],r[0][2]]
	#求取距离pt距离最近的站点的坐标和次序
	for i in range(len(r)):
		currentDist = dist2(pt,[r[i][1],r[i][2]])
		if currentDist <= minDist:
			minDist = currentDist
			stop = [r[i][0],r[i][1],r[i][2]]
	return stop
	
#根据guid的刷卡时间，求每一个bus_id每一天的到站时间、方向、坐标、次序，上车人数
def save_BusStop():
	busIds = afcBusId()
	for bus_id in busIds:
		days = afc_day(bus_id)
		for day in days:
			firstTimes = afc_time(bus_id,day)
			for time in firstTimes:
				loc =  afcLoc(bus_id,day,time[0])#查询距离上车时间最近的点
				stop = afcStop(bus_id,loc[0],loc[1])#查询距离上车点距离最近的站
				save_sql = 'INSERT INTO BUS_STOP values (?, ?, ?, ?, ?, ?, ?, ?, ?)'
				data = [(None,bus_id,day,time[0],loc[0],stop[1],stop[2],stop[0],time[1])]
				db.save(conn,save_sql,data)
	conn.commit()
	
#存储AFC_DATA_RIDE，求每一个guid的上车方向，站点，次序，同一站点上车人数
def save_AfcRide():
	sel_sql = 'SELECT * FROM AFC_DATA_ABOARD ORDER BY id ASC'
	cu.execute(sel_sql)
	r = cu.fetchall()
	for i in range(len(r)):
		sel_geo = 'SELECT * FROM BUS_STOP WHERE bus_id = {} AND day = {} AND time = {}'.format(r[i][5],r[i][3],r[i][4])
		cu.execute(sel_geo)
		geo = cu.fetchall()
		#存储每一条刷卡记录的上车站点
		save_sql = 'INSERT INTO AFC_DATA_RIDE values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
		data = [(None,r[i][1],r[i][2],r[i][3],r[i][4],r[i][5],geo[0][4],geo[0][5],geo[0][6],geo[0][7])]
		db.save(conn,save_sql,data)
	conn.commit()
	
#创建数据表CARDID_RATE，存每个card_id在每个上车点的上车次数
def create_RateTable():
	create_table_sql = '''CREATE TABLE `CARDID_RATE`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`card_id` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL,
						`count` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)
	
#计算each card_id 的上车站的频次
def save_rateOfRide():
	cardIds = afcCardId()
	for _id in cardIds:
		sel_sql = 'SELECT lng,lat FROM AFC_DATA_RIDE WHERE card_id = {} ORDER BY id ASC'.format(_id)
		cu.execute(sel_sql)
		r = cu.fetchall()
		loc = []
		#存储每个站点的上车次数
		for i in r:
			if not i in loc:
				loc.append(i)
				save_sql = 'INSERT INTO CARDID_RATE values (?, ?, ?, ?, ?)'
				data = [(None,_id,i[0],i[1],r.count(i))]
				db.save(conn,save_sql,data)
	conn.commit()
	
#根据概率判断非连续(单独)刷卡记录的下车点。方法是求取当前上车点下游5到15站中上车人数最多的站点，以此为下车点。此判断的根据是假设某一站点对乘客上车和下车的吸引力是一样的。
def probAlight(bus_id,day,direction,sequence):
	sel_sql = 'SELECT lng,lat,sequence FROM BUS_STOP WHERE bus_id = {} AND day = {} AND direction = {} AND sequence >= {} ORDER BY count ASC'.format(bus_id,day,direction,sequence)
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 0:
		loc = [r[-1][0],r[-1][1]]
		i = len(r)-1
		while i >= 0:
			#此处5到15两个参数是关键参数，修改此参数，会导致单一刷卡记录的乘客的下车站结果的变化。
			if r[i][2] - sequence >= 5 and r[i][2] - sequence <= 15:
				loc = [r[i][0],r[i][1]]
				break
			else:
				i -= 1
		return loc
	else:
		return[0.0,0.0]
	
#根据card_id 和 上车点，判断最大可能下车点。若此card_id有多天记录，查询最多次数的上车点(除了本次上车点外)，以此上车点为本次的下车点；若无多天记录，则根据probAlight计算下车点
def inferAlight(val):
	sel_sql = 'SELECT lng,lat FROM CARDID_RATE WHERE card_id = {} ORDER BY count ASC'.format(val[2])
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 1:
		if r[-1][0] == val[7] and r[-1][1] == val[8]:
			return [r[-2][0],r[-2][1]]
		else:
			return [r[-1][0],r[-1][1]]
	else:
		loc = probAlight(val[5],val[3],val[6],val[9])
		if loc == [0.0,0.0]:
			loc = [val[7],val[8]]
		return loc

#计算each card_id 的 day
def cardId_Day(card_id):
	days = []
	sel_sql = 'SELECT day FROM AFC_DATA_RIDE WHERE card_id = {} GROUP BY day'.format(card_id)
	cu.execute(sel_sql)
	r = cu.fetchall()
	if len(r) > 0:
		for i in range(len(r)):
			days.append(r[i][0])
	return days
	
#创建数据库表RESULT_ALIGHT_LIST，存储下车点结果. lng1 lat1为上车点
def create_AlightTable():
	create_table_sql = '''CREATE TABLE `RESULT_ALIGHT_LIST`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`guid` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL,
						`lng1` float(11) NOT NULL,
						`lat1` float(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)
	
#计算each guid的下车点
def save_Alighting():
	cardIds = afcCardId()
	#根据card_id循环，分为多条记录或1条记录两种情况
	for _id in cardIds:
		sel_sql = 'SELECT * FROM AFC_DATA_RIDE WHERE card_id = {} ORDER BY day,aboard_time ASC'.format(_id)
		cu.execute(sel_sql)
		r = cu.fetchall()
		if len(r) > 1:
			days = cardId_Day(_id)
			save_sql = 'INSERT INTO RESULT_ALIGHT_LIST values (?, ?, ?, ?, ?, ?)'
			#分天循环，分为某天有多条记录或1条记录两种情况
			for day in days:
				sel_sql = 'SELECT * FROM AFC_DATA_RIDE WHERE card_id = {} AND day = {} ORDER BY aboard_time ASC'.format(_id,day)
				cu.execute(sel_sql)
				v = cu.fetchall()
				#若有多条记录，认为乘客的出行按时间形成一个出行链，下次上车点即为本次的下车点，第一次的上车点为最后一次的下车点。
				if len(v) > 1:
					for i in range(len(v)):
						if i < len(v)-1:
							geo = afcStop(v[i][5],v[i][6],[v[i+1][7],v[i+1][8]])
						else:
							geo = afcStop(v[i][5],v[i][6],[v[0][7],v[0][8]])
						data = [(None,v[i][1],geo[1],geo[2],v[i][7],v[i][8])]
						db.save(conn,save_sql,data)
				#若一天有1条记录，根据inferAlight判断下车点
				else:
					fellow = inferAlight(v[0])
					geo = afcStop(v[0][5],v[0][6],fellow)
					data = [(None,v[0][1],geo[1],geo[2],v[0][7],v[0][8])]
					db.save(conn,save_sql,data)
		#若一card_id只有1条记录，根据probAlight计算下车点
		else:
			save_sql = 'INSERT INTO RESULT_ALIGHT_LIST values (?, ?, ?, ?, ?, ?)'
			geo = probAlight(r[0][5],r[0][3],r[0][6],r[0][9])
			if geo == [0.0,0.0]:
				continue
			data = [(None,r[0][1],geo[0],geo[1],r[0][7],r[0][8])]
			db.save(conn,save_sql,data)
	print 'near over!'
	conn.commit()
	
#输出Alighting的结果
def write_AlightingResult():
	sel_sql = 'SELECT * FROM RESULT_ALIGHT_LIST'
	cu.execute(sel_sql)
	alightResult = cu.fetchall()
	with open(r'RESULT_ALIGHT_LIST.csv','wb') as f:
		writer = csv.writer(f,dialect='excel')
		for i in range(len(alightResult)):
			writer.writerow([alightResult[i][1],alightResult[i][2],alightResult[i][3]])
	f.close()

def main():
	print time.ctime()
	global db,conn,cu
	db = SqliteDB(r'testGPS.db')
	conn = db.get_conn()
	cu = db.get_cursor(conn)
	create_AboardTable()
	create_BusStopTable()
	create_RideTable()
	create_RateTable()
	create_AlightTable()
	id_sql = 'CREATE INDEX cardIdx on AFC_DATA (card_id)'
	cu.execute(id_sql)
	id_sql = 'CREATE INDEX afcIdx on AFC_DATA (bus_id,day,time)'
	cu.execute(id_sql)
	id_sql = 'CREATE INDEX result1Idx on RESULT_STOP_LIST (route_id,direction)'
	cu.execute(id_sql)
	conn.commit()
	print u'开始计算上车时间对应的站点信息 {}'.format(time.ctime())
	save_BusStop()
	id_sql = 'CREATE INDEX stopIdx on BUS_STOP (bus_id,day,time,direction,sequence)'
	cu.execute(id_sql)
	conn.commit()
	print u'结束计算上车时间对应的站点信息 {}'.format(time.ctime())
	print u'开始计算each guid的上车站点 {}'.format(time.ctime())
	save_AfcRide()
	id_sql = 'CREATE INDEX rideIdx on AFC_DATA_RIDE (card_id,day,aboard_time)'
	cu.execute(id_sql)
	conn.commit()
	print u'结束计算each guid的上车站点 {}'.format(time.ctime())
	print u'开始计算each card_id的下车站点比率 {}'.format(time.ctime())
	save_rateOfRide()
	id_sql = 'CREATE INDEX rateIdx on CARDID_RATE (card_id)'
	cu.execute(id_sql)
	conn.commit()
	print u'结束计算each card_id的下车站点比率 {}'.format(time.ctime())
	print u'开始计算each guid的下车站点 {}'.format(time.ctime())
	save_Alighting()
	write_AlightingResult()
	print u'结束计算each guid的下车站点 {}'.format(time.ctime())
	db.close_all(conn,cu)
	print time.ctime()

if __name__ == '__main__':
	main()
