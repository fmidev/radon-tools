import sys
import datetime
import optparse
import csv
import psycopg2
#import ppygis
import collections

from optparse import OptionParser

conn = None
cur = None
metaIds = {}

all_columns = ["producer_id", 
					"analysis_time", 
					"station_id", 
					"param_id", 
					"forecast_period", 
					"latitude", 
					"longitude", 
					"level_id", 
					"level_value",
					"forecast_type_id",
					"forecast_type_value",
					"value"]

# create a list of required columns

required_columns = all_columns
required_columns.remove("forecast_type_value")

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	desc = """
Load previ csv to neon2.

Program must know the values of the following metadata parameters. Values can be specified either in command line or at csv. Values at command line overwrite csv.

producer_id: Id of producer as found from table fmi_producer. Command line or CSV.
"""

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0", description=desc)
			  
	parser.add_option("-v", "--verbose",
					  action="store_true",
					  default=False,
					  help="Set verbose mode on")

	parser.add_option("-r", "--producer_id",
					  action="store",
					  type="int",
					  help="Producer id",)
	
	parser.add_option("-P", "--forecast_period",
					  action="store",
					  type="int",
					  help="Forecast period (offset from analysis time)",)
	
	parser.add_option("-p", "--param_id",
					  action="store",
					  type="int",
					  help="Parameter id",)
	
	parser.add_option("--station_id",
					action="store",
					type="int",
					help="Station id")

	parser.add_option("--latitude",
					action="store",
					type="float",
					help="Latitude (WGS-84, DD.DDDDD)")

	parser.add_option("--longitude",
					action="store",
					type="float",
					help="Longitude (WGS-84, DD.DDDDD")

	parser.add_option("--level_value",
					action="store",
					type="float",
					help="Level value")

	parser.add_option("--level_id",
					action="store",
					type="int",
					help="Level id")

	parser.add_option("-f", "--format",
					  action="store",
					  type="string",
					  help="Format (ordering) of CSV columns")

	parser.add_option("--forecast_type_id",
					  action="store",
					  type="int",
					  help="Forecast type id (from table forecast_type)")

	parser.add_option("--forecast_type_value",
					  action="store",
					  type="float",
					  help="Forecast type value (optional)")

	parser.add_option("-a", "--analysis_time",
					  action="store",
					  type="string",
					  help="Analysis time, YYYY-MM-DDTHH24:MI:SS")

	(options, arguments) = parser.parse_args()
  
	# Check requirements

	if not options.format:
		print "Argument -f must be specified"
		parser.print_help()
		sys.exit(2)

	format_columns = options.format.split(',')

	options_dict = vars(options)

	# check that all columns listed with -f are valid

	for column in format_columns:
		if column not in all_columns:
			print "Unknown column specified: %s" % (column)
			sys.exit(2)

	print format_columns
	for column in set(required_columns) - set(format_columns):
		try:
			if options_dict[column] == None:
				print "CSV column %s not specified with command line argument or with switch -f" % (column)
				sys.exit(2)

		except KeyError:
				print column 
				if column not in ["latitude","longitude","station_id"]:
					print "Accessing CSV column %s that has no corresponding command line option" % (column)
					sys.exit(2)
		
		# check that no unknown columns were given with -f


	if len(arguments) == 0:
		print "No input files given"
		sys.exit(2)
			
	
	return (options,arguments)

# GetStationInfo()
#
# Get station information (especially target_id) from database

def GetStationInfo(station,stationtype):

	query = "SELECT target_id,station_id,name,network_id FROM verifng.locations WHERE network_id = :nId AND station_id = :stId"

	cur.execute(query, { 'nId' : stationtype, 'stId' : station })

	rows = cur.fetchone()

	if rows == None or len(rows) == 0:
		print "Station with Id %d, type %d not found!" % (station,stationtype)
		sys.exit(1)
  
	return rows

def GetMetaIds():

	query = """
SELECT 
	m.id, 
	m.producer_id,
	m.param_id, 
	m.level_id, 
	m.level_value, 
	m.station_id, 
	st_x(m.position) AS longitude, 
	st_y(m.position) AS latitude 
FROM 
	previ_meta m""" 

	global metaIds
  
	cur.execute(query)

	rows = cur.fetchall()

	for row in rows:
		# producer_id-param_id-level_id-level_value-station_id-longitude-latitude
		key = "%s_%s_%s_%s_%s" % (row[1], row[2], row[3], row[4], row[5], row[6], row[7])

		metaIds[key] = row[0]

	print 'Read ' + str(len(metaIds)) + ' meta ids' #  for producer %s' % (producer_id)


def InsertMetaId(cols):
	query = """
INSERT INTO previ_meta (
	producer_id, 
	param_id,
	level_id,
	level_value,
	station_id,
	position) 
VALUES (%s, %s, %s, %s, %s, ST_PointFromText(%s, 4326))
RETURNING id"""

  	pos = None

  	if cols["latitude"] != None and cols["longitude"] != None:
  		pos = "POINT(%s %s)" % (cols["longitude"], cols["latitude"])
	
	cur.execute(query, (cols["producer_id"], cols["param_id"], cols["level_id"], cols["level_value"], cols["station_id"], pos))
  
	global metaIds
  
	row = cur.fetchone()

	metaIds[str('_'.join(cols))] = row[0]
  
	return row[0]


def ReadFile(options, file):
	
	inserts = 0
	updates = 0

	reader = csv.reader(open(file, 'rb'), delimiter=',')

	totalrows = 0

	producer_id = None

	staticols = {}

	if options.analysis_time != None:
		staticols["analysis_time"] = datetime.datetime.strptime(options.analysis_time, '%Y-%m-%dT%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

	if options.producer_id != None:
		staticols["producer_id"] = options.producer_id

	if options.param_id != None:
		staticols["param_id"] = options.param_id

	if options.forecast_period != None:
		staticols["forecast_period"] = options.forecast_period

	if options.station_id != None:
		staticols["station_id"] = options.station_id

	if options.level_id != None:
		staticols["level_id"] = options.level_id

	if options.level_value != None:
		staticols["level_value"] = options.level_value

	if options.forecast_type_id != None:
		staticols["forecast_type_id"] = options.forecast_type_id

	if options.forecast_type_value != None:
		staticols["forecast_type_value"] = options.forecast_type_value
	
	staticols["forecast_type_value"] = None

	if options.forecast_type_value != None:
		staticols["forecast_type_value"] = options.forecast_type_value

	for line in reader:

		if len(line) == 0 or line[0][0].strip() == '#':
			continue

		totalrows += 1
		
		# ordered dict has keys always in same order when serialized
		# --> our key is guaranteed to be always the same (in same order)

		cols = collections.OrderedDict(staticols)

		i = 0

		for col in options.format.split(','):
			if not col in cols:
				cols[col] = None if line[i].strip() == "" else line[i].strip()
			print col + " --> " + str(line[i])

			i += 1

		if (cols["latitude"] != None and cols["longitude"] == None) or (cols["latitude"] == None and cols["longitude"] != None):
			print "Error, both latitude and longitude must be specified, or neither"
			continue

		if cols["station_id"] == None and (cols["latitude"] == None and cols["longitude"] == None):
			print "Error, either station_id or coordinates or both must be specified"
			continue

		# generate key that uniquely identifies a previ_meta id
		key = '_'.join(cols)
		#[str(cols["producer_id"]),
		#				str(cols["analysis_time"]),
		#				str(cols["param_id"]),
		#				str(cols["level_id"]),
		#				str(cols["level_value"]),
		#				str(cols["forecast_period"]),
		#				str(cols["station_id"]),
		#				str(cols["longitude"]),
		#				str(cols["latitude"]),
		#				str(cols["forecast_type_id"]),
		#				str(cols["forecast_type_value"])])
		print key
		# primary key columns must be non-null

		if cols["producer_id"] == None or \
			cols["param_id"] == None or \
			cols["analysis_time"] == None or \
			cols["level_id"] == None:
			print "Mandatory columns (producer_id,param_id,analysis_time,level_id) cannot be NULL"
			print cols
			continue

		print key

		try:
			meta_id = metaIds[key]		
		except:
			InsertMetaId(cols)

		try:
			query = "INSERT INTO ASD (forecast_id,fs_id,value) VALUES (:fcId,:fsId,:v)"

			cur.execute(query, {'fcId' : fcId, 'fsId' : fsId, 'v' : value})

			inserts += 1;

		except:
	#		query = "UPDATE verifng." + destination + " SET value = :v WHERE forecast_id = :fcId AND fs_id = :fsId"
	  
#			cur.execute(query, {'fcId' : fcId, 'fsId' : fsId, 'v' : value,})
	#		updates += 1;

			if totalrows % 100 == 0:
				print str(datetime.datetime.now()) + " cumulative inserts: " + str(inserts) + ", updates: " + str(updates)
				conn.commit()
	
	conn.commit()
	conn.close()

	print "Did",inserts,"inserts,",updates,"updates"

# Main body

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])
	
	username = 'wetodb'
	password = '3loHRgdio'
	hostname = 'dbdev.fmi.fi'
	port = 5432

	print "Connecting to database neon2 at host %s port %d" % (hostname, port)

	dsn = "user=%s password=%s host=%s dbname=neon2 port=%d" % (username, password, hostname, port)
	conn = psycopg2.connect(dsn)

	conn.autocommit = 0

	cur = conn.cursor()

# 	GetProducer(producer)
	# stationInfo = GetStationInfo(arguments.station,arguments.stationtype)

	#GetForecastIds(producer)
	#GetForecastInstanceIds(stationInfo[0])

	GetMetaIds()

	for file in files:
		print "Reading file '%s'" % (file)
		ReadFile(options, file)	
