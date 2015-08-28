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

'''第一部分：计算站点。步骤如下：
1.首先将GPS轨迹数据和BUS_ROUTE_DIC数据存入数据库；
2.对GPS数据进行去噪处理. —— gpsFilter(tableName)；
3.按行进方向对GPS数据进行分类，分类后的数据存入新表. —— classified()；
4.对同一route_id的每一个direction的数据，按bus_id进行分别求出滞留点(特征子集)，并聚类求出可能站点. —— get_PtsFilter(busids,direction)；
5.对每一个route_id的所有bus_id的聚类结果再次聚类，求出每一条route_id的站点. —— save_StopResult()；
6.对求出的站点中距离过近的再次求中心点 —— reCore(clusters)，并对结果进行排序. —— stopList(bus_id,direction,corePts)；
7.输出站点计算的结果。'''

#创建存储GPS_DATA的表，用来存储GPS_DATA.csv,`id`为自增长主键
def create_GPSTable():
	create_table_sql = '''CREATE TABLE `GPS_DATA`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`bus_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`time` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)

#存储GPS_DATA.csv的数据
def save_GPS():
	save_sql = '''INSERT INTO GPS_DATA values (?, ?, ?, ?, ?, ?)'''
	with open(r'/home/public/data/GPS_DATA.csv','rb') as f:
		reader = csv.reader(f)
		num = 0
		for row in reader:
			if num == 0:#第一条为标题，不需要存储
				num += 1
			else:
				if (float(row[3])>=113.8 and float(row[3])<=114.2) or(float(row[4])>=22.4 and float(row[4])<=22.7):
				#对GPS数据进行初步筛选，除去不在路网范围的被噪音点。路网经纬度范围由ROAD_NETWORK_SAMPLE.geojson转成的shp文件得知。
					data = [(None,int(row[0]),int(row[1]),int(row[2]),float(row[3]),float(row[4]))]
					db.save(conn,save_sql,data)
	conn.commit()

#创建存储GPS_DATA_CLASSIFY的表，存储按往返类型分类后的数据，`direction`为行进方向，其余项目与GPS_DATA.CSV相同
def create_ClassifyTable():
	create_table_sql = '''CREATE TABLE `GPS_DATA_CLASSIFY`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`bus_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`time` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL,
						`direction` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)

#创建存储BUS_ROUTE_DIC.csv数据的表
def create_Bus_RouteTable():
	create_table_sql = '''CREATE TABLE `BUS_ROUTE_DIC`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`bus_id` int(11) NOT NULL,
						`route_id` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)

#存储BUS_ROUTE_DIC.csv的数据
def save_ROUTE():
	save_sql = '''INSERT INTO BUS_ROUTE_DIC values (?, ?, ?)'''
	with open(r'/home/public/data/BUS_ROUTE_DIC.csv','rb') as f:
		reader = csv.reader(f)
		num = 0
		for row in reader:
			if num == 0:
				num += 1
			else:
				data = [(None,int(row[0]),int(row[1]))]
				db.save(conn,save_sql,data)
	conn.commit()

#创建存储AFC_DATA.csv数据的表
def create_AfcTable():
	create_table_sql = '''CREATE TABLE `AFC_DATA`(
						`id` INTEGER PRIMARY KEY autoincrement,
						`guid` int(11) NOT NULL,
						`card_id` int(11) NOT NULL,
						`day` int(11) NOT NULL,
						`time` int(11) NOT NULL,
						`bus_id` int(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)

#存储AFC_DATA.csv的数据
def save_AFC():
	save_sql = '''INSERT INTO AFC_DATA values (?, ?, ?, ?, ?, ?)'''
	with open(r'/home/public/data/AFC_DATA.csv','rb') as f:
		reader = csv.reader(f)
		num = 0
		for row in reader:
			if num == 0:
				num += 1
			else:
				data = [(None,int(row[0]),int(row[1]),int(row[2]),float(row[3]),float(row[4]))]
				db.save(conn,save_sql,data)
	conn.commit()

#创建存储站点结果的表RESULT_STOP_LIST
def create_StoplistTable():
	create_table_sql = '''CREATE TABLE `RESULT_STOP_LIST`(
						`stop_id` INTEGER PRIMARY KEY autoincrement,
						`route_id` int(11) NOT NULL,
						`direction` int(11) NOT NULL,
						`sequence` int(11) NOT NULL,
						`lng` float(11) NOT NULL,
						`lat` float(11) NOT NULL
						)'''
	db.create_table(conn,create_table_sql)
	
#对GPS点数据进行去噪处理，去除总点数过少和速度过大的噪点。tableName为要去噪的数据库表的名称
def gpsFilter(tableName):
	getBusID_sql = 'SELECT bus_id FROM {} GROUP BY bus_id'.format(tableName)
	cu.execute(getBusID_sql)
	r = cu.fetchall()
	if len(r) > 0:
		#按`bus_id`进行循环
		for e in range(len(r)):
			selete_sql = 'SELECT * FROM {} WHERE bus_id = {} ORDER BY day, time ASC'.format(tableName,r[e][0])
			cu.execute(selete_sql)
			input = cu.fetchall()
			if len(input) <= 100:#去除总点数过少(100一下)的GPS点
				del_sql = 'DELETE FROM {} WHERE bus_id = {}'.format(tableName,r[e][0])
				cu.execute(del_sql)
			dayNow = input[0][2]
			for i in range(len(input)):
				if i >0:
					if input[i][2] == dayNow:
						#对每一天的数据进行速度筛选，去除速度大于80km/h(22.2m/s)的GPS点
						speed = dist2([input[i-1][4],input[i-1][5]],[input[i][4],input[i][5]])/(input[i][3]-input[i-1][3])
						if speed > 22.2:
							del_sql = 'DELETE FROM {} WHERE id = {}'.format(tableName,input[i][0])
							cu.execute(del_sql)
					else:
						dayNow = input[i][2]
	conn.commit()

#对GPS的点，去掉超过速度限制的点。tableName数据库表名，datarow是GPS_DATA表的某bus_id某day的数据，limit是速度的限制(单位m/s)
def speedFilter(tableName,dataRow,limit):
	currentDay = dataRow[0][2]
	for j in range(len(dataRow)):
		if j > 0:
			if dataRow[j][2] == currentDay:
				speed = dist2([dataRow[j-1][4],dataRow[j-1][5]],[dataRow[j][4],dataRow[j][5]])/(dataRow[j][3]-dataRow[j-1][3])
				if speed > limit:
					del_sql = 'DELETE FROM {} WHERE id = {}'.format(tableName,dataRow[j][0])
					cu.execute(del_sql)
			else:
				currentDay = dataRow[j][2]

#对去噪后的GPS点数据按往返进行分类
def classified():
	getBusID_sql = 'SELECT bus_id FROM GPS_DATA GROUP BY bus_id'
	cu.execute(getBusID_sql)
	r = cu.fetchall()
	getDay_sql = 'SELECT day FROM GPS_DATA GROUP BY day'
	cu.execute(getDay_sql)
	d = cu.fetchall()
	if len(r) > 0:
		for e in range(len(r)):
			start_endPts = []#存储可能是往返点的坐标
			#按照bus_id和day循环，找出each bus_id each day的折返点
			for v in range(len(d)):
				selete_sql = 'SELECT * FROM GPS_DATA WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(r[e][0],d[v][0])
				cu.execute(selete_sql)
				input = cu.fetchall()
				for i in range(len(input)):
					if i >= 1:
						p1 = [input[i-1][4],input[i-1][5]]
						p2 = [input[i][4],input[i][5]]
						#若一个点停车时间超过6分钟，且与上一个点的距离小于1前面，则认为改点很可能是折返点
						if input[i][3] - input[i-1][3] > 360 and dist2(p1,p2) < 1000:
							start_endPts.append(p1)
			#s_ePoints是根据可能的折返点集合start_endPts求出的起点和终点的坐标
			s_ePoints = stopLoc(start_endPts)

			#以下是根据起点和终点对GPS点进行方向的分类
			for j in range(len(d)):
				selete_sql = 'SELECT * FROM GPS_DATA WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(r[e][0],d[j][0])
				cu.execute(selete_sql)
				filter = cu.fetchall()
				if len(filter) == 0:
					continue
				pointStart = [filter[0][4],filter[0][5]]
				#如果第一个点和起点的距离小于其和终点的距离，认为第一个点的direction为0，反之则为1
				if dist2(pointStart,s_ePoints[0]) < dist2(pointStart,s_ePoints[1]):
					direction = 0
				else:
					direction = 1
				for k in range(len(filter)):
					#存储第一个点的行进方向
					if k < 1:
						save_sql = 'INSERT INTO GPS_DATA_CLASSIFY values (?, ?, ?, ?, ?, ?, ?)'
						data = [(None,filter[k][1],filter[k][2],filter[k][3],filter[k][4],filter[k][5],direction)]
						db.save(conn,save_sql,data)
						continue
					'''多个条件判断一个点是否为折返点。折返点需要满足：
					1.停车250秒以上，与前一个点距离1千米以内。过滤掉普通站点停靠和隧道接收不到GPS信号的情况；
					2.和起点或者终点距离1千米以内，距离上一个折返点距离大于3500米。过滤掉堵车或等红绿灯的情况，过滤同一个起点(终点)站有多个满足条件1的GPS点的情况；
					3.和此天最后一个点的个数距离大于100个点。如折返点后只有点数不足100的点，则认为这些最后的点的行进方向未变化'''
					p1 = [filter[k-1][4],filter[k-1][5]]
					p2 = [filter[k][4],filter[k][5]]
					if filter[k][3]-filter[k-1][3] > 250 and dist2(p1,p2) < 1000 and (dist2(p1,s_ePoints[0])<1000 or dist2(p1,s_ePoints[1])<1000) and dist2(p1,pointStart) > 3500 and len(filter)-k>100:
						pointStart = p1
						if direction == 0:
							direction = 1
						else:
							direction = 0
						save_sql = 'INSERT INTO GPS_DATA_CLASSIFY values (?, ?, ?, ?, ?, ?, ?)'
						data = [(None,filter[k][1],filter[k][2],filter[k][3],filter[k][4],filter[k][5],direction)]
						db.save(conn,save_sql,data)
					else:
						save_sql = 'INSERT INTO GPS_DATA_CLASSIFY values (?, ?, ?, ?, ?, ?, ?)'
						data = [(None,filter[k][1],filter[k][2],filter[k][3],filter[k][4],filter[k][5],direction)]
						db.save(conn,save_sql,data)
	conn.commit()
	
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
	
#计算大圆距离的函数，单位是米
def dist1(p1, p2):
	if p1[0] == p2[0] and p1[1] == p2[1]:
		return 0.0
	else:
		x1 = math.radians(p1[0])
		x2 = math.radians(p2[0])
		y1 = math.radians(90 - p1[1])
		y2 = math.radians(90 - p2[1])
		c = math.sin(y1)*math.sin(y2)*math.cos(x1-x2)+math.cos(y1)*math.cos(y2)
		dis = 6771004*math.acos(c)
		return dis

#投影后计算平面距离的函数，单位是米
def dist2(p1,p2):
	if p1[0] == p2[0] and p1[1] == p2[1]:
		return 0.0
	else:
		x_y1 = GeodeticToCartesian(p1)
		x_y2 = GeodeticToCartesian(p2)
		dis = math.sqrt((x_y1[0]-x_y2[0])*(x_y1[0]-x_y2[0]) + (x_y1[1]-x_y2[1])*(x_y1[1]-x_y2[1]))
		return dis
	
#求出可能折返点集合points的两个中心点，目的是求出each bus_id的起点和终点的位置
def stopLoc(points):
	stopPts = [[113.7,22.3],[114.3,22.8]]
	if len(points) == 2:
		stopPts = points 
	elif len(points) == 3:
		stopPts = maxDistPts(points)
	elif len(points) > 3:
		maxPoint = maxDistPts(points)
		tempPts = coreLoc(points,maxPoint)
		stopPts = coreLoc(points,tempPts)
	return stopPts

#求一系列点points中任意两个点的距离的最大值，即最远距离
def maxDist(points):
	maxDistance = 0
	for id1,p1 in enumerate(points):
		for id2,p2 in enumerate(points):
			if id1 < id2:
				if maxDistance < dist2(p1,p2):
					maxDistance = dist2(p1,p2)
	return maxDistance
	
#求一系列点points中距离最远的两个点
def maxDistPts(points):
	maxDistance = 0
	p0 = []
	pn = []
	for id1,p1 in enumerate(points):
		for id2,p2 in enumerate(points):
			if id1 < id2:
				if maxDistance < dist2(p1,p2):
					maxDistance = dist2(p1,p2)
					p0 = p1
					pn = p2
	return [p0,pn]
	
#求取maxDistPt中每个点缓冲区内的points点的中心
def coreLoc(points,maxDistPt):
	pStart = []
	pEnd = []
	StartEnd = []
	if maxDist(points) < 1500:
		return maxDistPt
	for point in points:
		#缓冲区半径为1.5千米
		if dist2(point,maxDistPt[0]) < 1500:
			pStart.append(point)
			continue
		if dist2(point,maxDistPt[1]) < 1500:
			pEnd.append(point)
	if len(pStart) < 1 or len(pEnd) < 1:
		return maxDistPt
	x = y = 0.0
	for s in pStart:
		x += s[0]
		y += s[1]
	meanPoint1 = [x/len(pStart),y/len(pStart)]
	
	x = y = 0.0
	for e in pEnd:
		x += e[0]
		y += e[1]
	meanPoint2 = [x/len(pEnd),y/len(pEnd)]

	if meanPoint1[0] < meanPoint2[0]:#以lng小的为起点站
		StartEnd.append(meanPoint1)
		StartEnd.append(meanPoint2)
	else:
		StartEnd.append(meanPoint2)
		StartEnd.append(meanPoint1)
	return StartEnd
	
#DBSCAN的函数，求出每一簇的中心点。
def dbscan(all_points,Eps,minPts):
	for point in all_points:
		point[2] = 0# 指定初始簇标签为0
	'''定义邻域的半径为Eps，邻域内相邻数据点个数大于minPts的为核心点.Eps和minPts是关键参数，这两个参数细微的变化也可能导致结果的不同。
	由于每一bus_id、每一天,乃至于每一趟的GPS点的数目都是变化的，而我们不可能每一次聚类时都修改minPts的值，但是dbscan对参数的变化很敏感，所以固定一个
	数值也会导致聚类的结果不好，在下面调用dbscan函数的时候，minPts是all_points的个数乘以一个比例。'''
	surroundPts = defaultdict(list)
	#找出核心点
	for idx1,point1 in enumerate(all_points):
		for idx2,point2 in enumerate(all_points):
			if(idx1 < idx2):
				#用经纬度差过滤，减少计算次数
				if abs(point1[0] - point2[0])<=0.006 and abs(point1[1] - point2[1])<=0.006:
					if dist2(point1,point2)<=Eps:
						surroundPts[idx1].append(idx2)
						surroundPts[idx2].append(idx1)
	
	corePtsIdx = [pointIdx for pointIdx,surPointIdxs in surroundPts.iteritems() if len(surPointIdxs)>=minPts]
	#找出边界点
	borderPtsIdx = []
	for pointIdx,surPointIdxs in surroundPts.iteritems():
		if(pointIdx not in corePtsIdx):
			for onesurPtIdx in surPointIdxs:
				if onesurPtIdx in corePtsIdx:
					borderPtsIdx.append(pointIdx)
	
	#算法的实现
	cluster_label=0#簇标签
	# 各个核心点与其邻域内的所有核心点放在同一个簇中
	for pointIdx,surPointIdxs in surroundPts.iteritems():
		for onesurPtIdx in surPointIdxs:
			if(pointIdx in corePtsIdx and onesurPtIdx in corePtsIdx and pointIdx < onesurPtIdx):
				if all_points[pointIdx][2] == 0:
					cluster_label += 1
					all_points[pointIdx][2] = cluster_label
				all_points[onesurPtIdx][2] = all_points[pointIdx][2]

	# 边界点跟其邻域内的某个核心点放在同一个簇中
	for pointIdx,surPointIdxs in surroundPts.iteritems():
		for onesurPtIdx in surPointIdxs:
			if(pointIdx in borderPtsIdx and onesurPtIdx in corePtsIdx):
				if all_points[onesurPtIdx][2] == 0:
					cluster_label += 1
					all_points[onesurPtIdx][2] = cluster_label
				all_points[pointIdx][2] = all_points[onesurPtIdx][2]

	#把标签相同的点组合在一起
	corePoint = [all_points[pointIdx] for pointIdx in corePtsIdx]
	borderPoint = [all_points[pointIdx] for pointIdx in borderPtsIdx]
	plotted_points = corePoint + borderPoint
	cluster_list = defaultdict(lambda: [[],[]])
	for point in plotted_points:
		cluster_list[point[2]][0].append(point[0])
		cluster_list[point[2]][1].append(point[1])
	#返回簇的中心，以及簇的点数目
	resultPts = []
	for value in cluster_list:
		x = y = 0.0
		cluster= cluster_list[value]
		for i in range(len(cluster[0])):
			x += cluster[0][i]
			y += cluster[1][i]
		tempPt = [x/len(cluster[0]),y/len(cluster[0]),len(cluster[0])]
		resultPts.append(tempPt)
	return resultPts

#求停滞点方法1；连续点序列满足，1.所有点落在一个空间区域内，2.Pi和Pi+1的时间差大于某一个值。此序列的中心即为停滞点
def method1(points,temporal,radius):
	stayPts = []
	if len(points) > 1:
		for i in range(len(points)):
			for j in range(len(points)):
				if j-1 > i and j-i < 11 and points[j-1][2] - points[i][2] >= temporal and maxDist(points[i:j]) <= 2*radius and maxDist(points[i:j+1]) > 2*radius:
					stayPts.extend(points[i:j])
	return stayPts

#求停滞点方法2；连续点序列满足加速度一直为负，停滞点为连续点序列中最后一个加速度为负的点
def method2(points):
	stayPts = []
	if len(points) > 1:
		speed = []
		speedNow = 0.0
		for i in range(len(points)):
			if i == 0:
				speed.append(speedNow)
				continue
			speedNow = dist2(points[i],points[i-1])/(points[i][2]-points[i-1][2])
			speed.append(speedNow)
		for j in range(len(speed)):
			if j > 1 and speed[j-1] <= 5.6 and speed[j-1] - speed[j-2] < 0 and speed[j] - speed[j-1] > 0:
				stayPts.append(points[j-1])
	return stayPts

#相同的route_id和direction，按bus_id循环，求满足method1(method2)的GPS点，对其进行dbscan聚类，求出中心点。本次选择method2求滞留点
def get_PtsFilter(busids,direction):
	all_points=[]
	for _id in busids:
		day_sql = 'SELECT day FROM GPS_DATA_CLASSIFY WHERE bus_id = {} GROUP BY day'.format(_id)
		cu.execute(day_sql)
		d = cu.fetchall()
		pts = []
		if len(d) > 0:
			for i in range(len(d)):
				getGPS_sql = 'SELECT lng, lat, time, direction FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(_id,d[i][0])
				cu.execute(getGPS_sql)
				r = cu.fetchall()
				'''dbscan的时间复杂度高，通过采用method1(method2)对聚类的点进行筛选，来提高效率；同时也提高了聚类结果的准确率。'''
				#data = method1(r,25,50)
				data = method2(r)
				for v in data:
					if v[3] == direction:
						pts.append([v[0],v[1],v[2]])

		minPts = int(0.005*len(pts))
		#聚类半径为50米，最小个数为minPts
		clusters = dbscan(pts,50,minPts)
		all_points.extend(clusters)
	return all_points
	
#each bus_id each day 的刷卡数据，求出大概的每一天停下的站点总次数
def afc_count(busid,day):
	#查询，计算afc数据中的时间间隔，以判断两个相邻站点之间的时间间隔
	getAfc_sql = 'SELECT * FROM AFC_DATA WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(busid,day)
	cu.execute(getAfc_sql)
	r = cu.fetchall()
	afc_stop = 1.0
	time0 = r[0][4]
	for j in range(len(r)):
		if j > 0:
			#认为与上次刷卡时间差超过20s，且与上一站第一次刷卡时间差超过90s的刷卡记录为下一站的第一次刷卡记录
			if r[j][4] - r[j-1][4] > 20 and r[j][4] - time0 > 90:
				time0 = r[j][4]
				afc_stop += 1.0
	return afc_stop
	
#求gps点数据each bus_id each day 的运行次数(一次往返为2次)
def reversePt(busid,day):
	getGPS_sql = 'SELECT * FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND day = {} ORDER BY time ASC'.format(busid,day)
	cu.execute(getGPS_sql)
	r = cu.fetchall()
	rePts = 1
	direction = 0
	for i in range(len(r)):
		if i > 0:
			if r[i-1][6] == direction and r[i][6] != direction:
				rePts += 1
				if direction == 0:
					direction = 1
				else:
					direction = 0
	return rePts

#获取某一route_id的大概公交站总数，方法是求同一route_id的每一个bus_id每一天的站数的平均值
def get_StopNum(routeid):
	busids = get_busId(routeid)
	stopNum = 0.0
	for _id in busids:
		day_sql = 'SELECT day FROM GPS_DATA_CLASSIFY WHERE bus_id = {} GROUP BY day'.format(_id)
		cu.execute(day_sql)
		day = cu.fetchall()
		num = 0.0
		if len(day) > 0:
			for i in range(len(day)):
				num += afc_count(_id,day[i][0])/reversePt(_id,day[i][0])#每一个天的停站次数除以每一天的运行次数，求出站点的总数
			num = num / len(day)
			stopNum += num
	stopNum = int(stopNum/len(busids))
	return stopNum
	
#对聚类的结果进行处理，把聚类中心间距小于200米的按照权重再求一次中心，权重是聚类中心的簇的点数目
def reCore(clusters):
	corePts = []
	for i in range(len(clusters)):
		temp = []
		for j in range(len(clusters)):
			if dist2(clusters[i],clusters[j]) <= 200:
				temp.append(clusters[j])
		if len(temp)>0:
			x = y = z = 0
			for k in range(len(temp)):
				x += temp[k][0]*temp[k][2]
				y += temp[k][1]*temp[k][2]
				z += temp[k][2]
			corePts.append([x/z,y/z])
		else:
			corePts.append([temp[0][0],temp[0][1]])
	uniquePts = []
	for value in corePts:
		if not value in uniquePts:
			uniquePts.append(value)
	return uniquePts
	
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
	
#对stoplist的站点进行排序，方法是求取每一个站点对应的时间，按照时间先后进行排序
def stopList(bus_id,direction,corePts):
	for core in corePts:
		core.append(0)
	sel_sql = 'SELECT lng,lat,time,id FROM GPS_DATA_CLASSIFY WHERE bus_id = {} AND day = 1 AND direction = {} ORDER BY time ASC'.format(bus_id,direction)
	cu.execute(sel_sql)
	pts = cu.fetchall()
	if len(pts) > 0:
		journey = pts[0:-1]
		for i in range(len(pts)):
			if pts[i][3] - pts[i-1][3] > 1:
				journey = pts[0:i]
		#求距离每一个站点最近的gps点的时间
		for j in range(len(corePts)):
			minDist = 1000
			minPt = journey[0]
			for k in range(len(journey)):
				if dist2(corePts[j],journey[k]) < minDist:
					minDist = dist2(corePts[j],journey[k])
					minPt = journey[k]
			corePts[j][2] = minPt[2]
	#按时间进行排序
	corePts.sort(key=lambda x:x[2])
	return corePts
	
#存储RESULT_STOP_LIST
def save_STOPLIST(route_id,direction,pts):
	save_sql = '''INSERT INTO RESULT_STOP_LIST values (?, ?, ?, ?, ?, ?)'''
	for i in range(len(pts)):
		data = [(None,route_id,direction,i+1,pts[i][0],pts[i][1])]
		db.save(conn,save_sql,data)
	conn.commit()
	
#计算所有线路的站点，并存储stop的结果
def save_StopResult():
	routeIds = get_routeId()
	#按route_id循环，求每一个route_id的两个方向的站点
	for routeId in routeIds:
		busIds = get_busId(routeId)
		#求direction == 0的站点
		pts = get_PtsFilter(busIds,0)
		clusters = dbscan(pts,50,int(0.008*len(pts)))
		corePts = stopList(busIds[0],0,reCore(clusters))
		save_STOPLIST(routeId,0,corePts)
		#求direction == 1的站点
		pts = get_PtsFilter(busIds,1)
		clusters = dbscan(pts,50,int(0.008*len(pts)))
		corePts = stopList(busIds[0],1,reCore(clusters))
		save_STOPLIST(routeId,1,corePts)
	conn.commit()
	
#输出Stop的结果
def write_StopResult():
	sel_sql = 'SELECT * FROM RESULT_STOP_LIST'
	cu.execute(sel_sql)
	stopData = cu.fetchall()
	with open(r'RESULT_STOP_LIST.csv','wb') as f:
		writer = csv.writer(f,dialect='excel')
		for i in range(len(stopData)):
			writer.writerow([stopData[i][0],stopData[i][1],stopData[i][2],stopData[i][3],stopData[i][4],stopData[i][5]])
	f.close()

def main():
	print time.ctime()
	global db,conn,cu
	db = SqliteDB(r'testGPS.db')
	conn = db.get_conn()
	cu = db.get_cursor(conn)
	create_GPSTable()
	save_GPS()
	create_Bus_RouteTable()
	save_ROUTE()
	create_AfcTable()
	save_AFC()
	id_sql = 'CREATE INDEX gpsIdx on GPS_DATA (bus_id, day, time)'
	cu.execute(id_sql)
	id_sql = 'CREATE INDEX routeIdx on BUS_ROUTE_DIC (route_id, bus_id)'
	cu.execute(id_sql)
	conn.commit()
	print u'结束数据存储 {}'.format(time.ctime())
	print u'开始去噪 {}'.format(time.ctime())
	create_ClassifyTable()
	gpsFilter('GPS_DATA')
	print u'结束去噪 {}'.format(time.ctime())
	print u'开始分方向 {}'.format(time.ctime())
	classified()
	create_StoplistTable()
	id_sql = 'CREATE INDEX clafyIdx on GPS_DATA_CLASSIFY (bus_id, day, time)'
	cu.execute(id_sql)
	conn.commit()
	print u'结束分方向 {}'.format(time.ctime())
	print u'开始计算站点 {}'.format(time.ctime())
	save_StopResult()
	write_StopResult()
	print u'结束计算站点 {}'.format(time.ctime())
	db.close_all(conn,cu)
	print time.ctime()

if __name__ == '__main__':
	main()
