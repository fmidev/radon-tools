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

	query = "SELECT lat/1e5,lon/1e5,nom_station FROM station WHERE id_station = :id_station"

	cur.prepare(query)
	cur.execute(None, {'id_station' : station_id})

	return cur.fetchone()


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

	print "previ version: %s" % (version)
	print "producer name: %s" % (ref_prod)
	print "producer id: %d" % (producer_id)
	print "station id: %s" % (station_id)
	print "initial time: %s" % (initial_time)
	print "created time: %s" % (created_time)
	print "num of data points: %d" % (num_points)

	cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='yyyy-mm-dd hh24:mi:ss'")

	stationinfo = GetStationInfo(station_id)

	query = "SELECT previ_id, tbl_name FROM as_previ WHERE ref_prod = :ref_prod AND :initial_time BETWEEN dat_debut_prevu AND dat_fin_prevu"

	cur.prepare(query)

	cur.execute(None, {'ref_prod' : ref_prod, 'initial_time' : initial_time})
	row = cur.fetchone()

	if row is None:
		print "Data set not found from Neons. Is the data too old?"
		return

	dset_id = row[0]
	tbl_name = row[1]

	print "dset_id: %s for table_name: %s" % (dset_id, tbl_name)
	
	cols = "watlev_cm"

	query = """
INSERT INTO %s ( 
  previ_id, 
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
  :dat_prevu, 
  :dat_run, 
  :echeance,
  :heure_res_run,
  :dat_insert,
  :value)""" % (tbl_name, cols)

	print query
	icur = conn.cursor()

	icur.prepare(query)
	
	query = """
UPDATE %s SET
  %s = :value
WHERE 
  previ_id = :previ_id
  AND
  lltbufr_val = :lltbufr_val
  AND
  dat_run = :dat_run
  AND
  dat_prevu = :dat_prevu
  AND
  dat_insert = :dat_insert
""" % (tbl_name, cols)	

	ucur = conn.cursor()
	ucur.prepare(query)

	totalrows = 0
	inserts = 0
	updates = 0

	for line in f:
		totalrows = totalrows + 1

		(forecast_time, value) = line.strip().split()
		value = float(value)
		
#		print "%s %f" % (forecast_time, value)

		echeance = int((datetime.datetime.strptime(forecast_time, '%Y%m%d%H%M%S') - datetime.datetime.strptime(initial_time, '%Y%m%d%H%M%S')).total_seconds()/3600)

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

		try:
			if not options.dry_run:
				icur.execute(None, 
					{	'previ_id' : dset_id, 
						'lat' : stationinfo[0], 
						'lon' : stationinfo[1], 
						'lltbufr_val' : station_id, 
						'dat_prevu' : forecast_time, 
						'dat_run' : initial_time, 
						'echeance' : echeance,
						'heure_res_run' : analysis_hour,
						'dat_insert' : datetime.datetime.now(),
						'value' : value
					})
				inserts = inserts + 1

		except:
			ucur.execute(None,
				{	'previ_id' : dset_id, 
					'lltbufr_val' : station_id, 
					'dat_prevu' : forecast_time, 
					'dat_run' : initial_time, 
					'dat_insert' : datetime.datetime.now(),
					'value' : value
				}
				)
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
