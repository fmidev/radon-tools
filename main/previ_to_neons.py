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
	query = "SELECT to_char(lat/1e5),to_char(lon/1e5),nom_station FROM station WHERE id_station = :id_station"

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
	print "producer name:\t%s" % (ref_prod)
	print "producer id:\t%d" % (producer_id)
	print "station id:\t%s" % (station_id)
	print "initial time:\t%s" % (initial_time)
	print "created time:\t%s" % (created_time)
	print "num of data points:\t%d" % (num_points)

	cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='yyyymmddhh24miss'")

	stationinfo = GetStationInfo(station_id)

	query = "SELECT previ_id, tbl_name FROM as_previ WHERE ref_prod = :ref_prod AND :initial_time BETWEEN dat_debut_prevu AND dat_fin_prevu"

	args = {'ref_prod' : ref_prod, 'initial_time' : initial_time}

	if options.show_sql:
		PrintQuery(query, args)
	
	cur.execute(query, args)

	row = cur.fetchone()

	if row is None:
		print "Data set not found from Neons. Is the data too old?"
		return

	dset_id = row[0]
	tbl_name = row[1]

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
INSERT INTO bdm.%s (previ_id, 
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
  :lat, 
  :lon, 
  :lltbufr_val, 
  to_date(:dat_prevu, 'yyyymmddhh24mi'), 
  to_date(:dat_run, 'yyyymmddhh24mi'),  
  :echeance,
  :heure_res_run,
  :dat_insert,
  %s)""" % (tbl_name, ','.join(cols), ','.join(binds))

	icur = conn.cursor()

	icur.prepare(iquery)
	
	uquery = """UPDATE %s SET
dat_insert = :dat_insert,
echeance = :echeance,
longitude = :lon,
latitude = :lat,
heure_res_run = :heure_res_run,
""" % (tbl_name)

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

	ucur = conn.cursor()
	ucur.prepare(uquery)

	totalrows = 0
	inserts = 0
	updates = 0

	for line in f:
		totalrows = totalrows + 1
		(forecast_time, value) = line.strip().split()

		if value == "32700.0":
			value = None
		else:
			value = Decimal(value)
		
#		print "%s %f" % (forecast_time, value)

		echeance = int(GetTotalSeconds(datetime.datetime.strptime(forecast_time, '%Y%m%d%H%M%S') - datetime.datetime.strptime(initial_time, '%Y%m%d%H%M%S'))/3600)

		if echeance < 0:
			print "Logical error in the data: forecast step is negative (%d hours)" % (echeance)
			continue

#		print dset_id
#		print stationinfo[0]
#		print stationinfo[1]
#		print station_id
#		print forecast_time
#		print initial_time
#		print echeance
#		print analysis_hour

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
			if options.show_sql:
				PrintQuery(iquery, args)

			if not options.dry_run:
				icur.execute(None, args)
				inserts = inserts + 1

		except Exception,e:
			
			if options.show_sql:
				PrintQuery(uquery, args)

			ucur.execute(None, args)
			updates = updates + 1

	conn.commit()
	conn.close()

	print "Did %d inserts, %d updates/%d rows" % (inserts, updates, totalrows)


# Main body

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])

	print "Connecting to database %s" % (options.database)

	password = "3loHRgdio"

	conn = cx_Oracle.connect(options.user, password, options.database)

	conn.autocommit = 0

	cur = conn.cursor()

	for file in files:
		print "Reading file '%s'" % (file)
		ReadFile(options, file)	
