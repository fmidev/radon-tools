#!/usr/bin/env python
#
# Insert previ to neons.
#
# Expected input file format is:
# ------------------------------------
# PreviVersion2
# BESTGU
# 2159
# 1
# 60
# -999
# 100063
# 201504161500 201502161754
# 2
# 201502161500 123.1
# 201502161600 111.8
# ------------------------------------

import re
import sys
import datetime
import optparse
import cx_Oracle
import getpass
import os

from decimal import Decimal
from optparse import OptionParser

conn = None
cur = None

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	desc = """
Load previ to neons.
"""

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0", description=desc)
			  
	databasegroup = optparse.OptionGroup(parser, "Options for database connection")

	parser.add_option("-v", "--verbose",
				  action="store_true",
				  default=False,
				  help="Set verbose mode on")

	parser.add_option("--show_sql",
					  action="store_true",
					  default=False,
					  help="Show SQL that's executed")

	parser.add_option("--dry_run",
					  action="store_true",
					  default=False,
					  help="Do not make any changes to database, sets --show_sql")

	databasegroup.add_option("--database",
					action="store",
					type="string",
					default="neons",
					help="Database name")

	databasegroup.add_option("--user",
					action="store",
					type="string",
					default="wetodb",
					help="Database username")


	databasegroup.add_option("--password",
				action="store",
				type="string",
				help="Database password")

	parser.add_option_group(databasegroup)

	(options, arguments) = parser.parse_args()
  
	if len(arguments) == 0:
		print "No input files given"
		parser.print_help()
		sys.exit(2)
			
	if options.dry_run:
		options.show_sql = True

	return (options,arguments)

def GetTotalSeconds(td): 
	# no timedelta.total_seconds() in python 2.6
	return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

def GetStationInfo(station_id):

	# use to_char to prevent floating point inaccuracies
	query = "SELECT to_char(lat/1e5),to_char(lon/1e5),nom_station FROM station WHERE to_number(indicatif_omm) = :id_station"

	args = {'id_station' : station_id}

	if options.show_sql:
		PrintQuery(query, args)
	
	cur.execute(query, args)

	return cur.fetchone()

def PrintQuery(query, args):
	print "%s;\nargs: %s" % (query, args)

def ReadFile(options, file):
	
	inserts = 0
	updates = 0

	f = open(file, 'r')

	version = f.readline().strip()
	ref_prod = f.readline().strip()
	producer_id = int(f.readline().strip())
	unknown_1 = f.readline().strip()
	unknown_2 = f.readline().strip()
	unknown_3 = f.readline().strip()
	station_id = f.readline().strip()

	tmp = f.readline().split()

	initial_time = tmp[0]
	created_time = tmp[1]

	analysis_hour = datetime.datetime.strptime(initial_time, '%Y%m%d%H%M%S').strftime('%H')

	num_points = int(f.readline().strip())

	# print "previ version: %s" % (version)
	print "Producer:\t%s %s" % (ref_prod, producer_id)
	print "Station id:\t%s" % (station_id)
	print "Initial, created time:\t%s %s" % (initial_time, created_time)
	print "Num of data points:\t%d" % (num_points)

	cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='yyyymmddhh24miss'")

	stationinfo = GetStationInfo(station_id)

	if stationinfo == None:
		print "ERR: station %s not found from neons table 'station'" % (station_id)
		return

	# print "dset_id: %s for table_name: %s" % (dset_id, tbl_name)
	query = "SELECT col_name FROM previ_col WHERE previ_type = :ref_prod ORDER BY ordre"
	args = {'ref_prod' : ref_prod}

	if options.show_sql:
		PrintQuery(query, args)

	cur.execute(query, args)

	cols = []

	for row in cur.fetchall():
		cols.append(row[0])

	binds = [':' + x for x in cols]

	iquery = """
INSERT INTO bdm.PREVI_TABLE (previ_id, 
  latitude, 
  longitude, 
  lltbufr_val, 
  dat_prevu, 
  dat_run, 
  echeance, 
  heure_res_run, 
  dat_insert, 
  %s
) VALUES (
  :previ_id, 
  1e5 * :lat, 
  1e5 * :lon, 
  :lltbufr_val, 
  to_date(:dat_prevu, 'yyyymmddhh24mi'), 
  to_date(:dat_run, 'yyyymmddhh24mi'),  
  :echeance,
  :heure_res_run,
  :dat_insert,
  %s)""" % (','.join(cols), ','.join(binds))

	uquery = """UPDATE PREVI_TABLE SET
dat_insert = :dat_insert,
echeance = :echeance,
longitude = :lon,
latitude = :lat,
heure_res_run = :heure_res_run,
"""

	for i, col in enumerate(cols):
		uquery += col + " = " + binds[i]

	uquery +="""
WHERE 
  previ_id = :previ_id
  AND
  lltbufr_val = :lltbufr_val
  AND
  dat_run = to_date(:dat_run, 'yyyymmddhh24mi')
  AND
  dat_prevu = to_date(:dat_prevu, 'yyyymmddhh24mi')""" 

	asquery = """
UPDATE bdm.as_previ SET
  rec_cnt = rec_cnt+1
WHERE
  previ_id = :previ_id  
"""

	totalrows = 0
	inserts = 0
	updates = 0

	for line in f:
		totalrows = totalrows + 1

		(forecast_time, value) = line.strip().split()

		# The handling of previ in neons is a bit strange:
		# The data is split to day-partitions based on forecast_timem not initial_time !

		query = "SELECT previ_id, tbl_name FROM as_previ WHERE ref_prod = :ref_prod AND :forecast_time BETWEEN dat_debut_prevu AND dat_fin_prevu"

		args = {'ref_prod' : ref_prod, 'forecast_time' : forecast_time}

		if options.show_sql:
			PrintQuery(query, args)

		cur.execute(query, args)

		row = cur.fetchone()

		if row is None:
			print "Data set not found from Neons for date %s. Is the data too old?" % (forecast_time)
			continue

		dset_id = row[0]
		tbl_name = row[1]

		if value == "32700.0":
			value = None
		else:
			value = Decimal(value)
		
		echeance = int(GetTotalSeconds(datetime.datetime.strptime(forecast_time, '%Y%m%d%H%M%S') - datetime.datetime.strptime(initial_time, '%Y%m%d%H%M%S'))/60) # minutes

		if echeance < 0:
			print "Logical error in the data: forecast step is negative (%d hours)" % (echeance)
			continue

		args = {
			'previ_id' : dset_id, 
			'lat' : (stationinfo[0]), 
			'lon' : (stationinfo[1]), 
			'lltbufr_val' : station_id, 
			'dat_prevu' : forecast_time, 
			'dat_run' : initial_time, 
			'echeance' : echeance,
			'heure_res_run' : analysis_hour,
			'dat_insert' : datetime.datetime.now()
		}

		# HUOM!
		# SKRIPTI OSAA TOISTAISEKSI LADATA VAIN SELLAISTA PREVIA JOSSA ON YKSI DATASARAKE!
		
		for i,col in enumerate(cols):
			args[col] = value #values[i]

		try:
			q = iquery.replace("PREVI_TABLE", tbl_name)

			if options.show_sql:
				PrintQuery(q, args)

			if not options.dry_run:
				cur.execute(q, args)
				inserts = inserts + 1

			# Update as_previ

			if options.show_sql:
				PrintQuery(asquery, {"previ_id" : dset_id})

			if not options.dry_run:
				cur.execute(asquery, {"previ_id" : dset_id})

		except Exception,e:

			print e
			q  = uquery.replace("PREVI_TABLE", tbl_name)
		
			if options.show_sql:
				PrintQuery(q, args)


			cur.execute(q, args)
			updates = updates + 1

	conn.commit()
	conn.close()

	print "Did %d inserts, %d updates/%d rows" % (inserts, updates, totalrows)


# Main body

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])

	print "Connecting to database %s" % (options.database)

	password = options.password

	if password is None:
		try:
			password = os.environ["NEONS_%s_PASSWORD" %(options.user.upper())]
		except:
			print "password should be given with env variable NEONS_%s_PASSWORD" % (options.user.upper())
			sys.exit(1)

	conn = cx_Oracle.connect(options.user, password, options.database)

	conn.autocommit = 0

	cur = conn.cursor()

	for file in files:
		print "Reading file '%s'" % (file)
		ReadFile(options, file)	
