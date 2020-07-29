#!/usr/bin/env python2
# interactive tool to automatically create geometries from grib files

import sys
from gribapi import *
import psycopg2

conn = None

def get_conn():
	global conn

	if conn is None:
		try:
			password = os.environ["RADON_WETODB_PASSWORD"]
		except:
			print "password should be given with env variable RADON_WETODB_PASSWORD"
			sys.exit(1)

		dsn = "user=wetodb password=%s host=vorlon dbname=radon port=5432" % (password)
		conn = psycopg2.connect(dsn)

	return conn

def update_table_meta_grid(grb, geom_id, name):
	conn = get_conn()
	cur = conn.cursor()

	prod_id = producer_id(grb)

	if prod_id is None:
		return

	query = "SELECT producer_id, schema_name, table_name, retention_period, partitioning_period, analysis_times FROM table_meta_grid WHERE producer_id = %s"

	cur.execute(query, (prod_id,))

	rows = cur.fetchall()
	
	if rows is None or len(rows) == 0:
		print "No earlier rows for producer %s found from table table_meta_grid, returning" % (prod_id)
		return

	for row in rows:
		print "Found existing definition from table_meta_grid for this producer but different geometry:"
		print "Producer: %s\nSchema: %s\nTable: %s\nRetention: %s\nPartitioning method: %s\nAnalysis_times: %s" % (row)

		proceed = raw_input("Insert line for geometry %s using these same attributes? " % (name)) 

		if len(proceed) == 0:
			return
		elif proceed != "y":
			continue
		
		cur.execute("INSERT INTO table_meta_grid(producer_id, schema_name, table_name, geometry_id, retention_period, partitioning_period, analysis_times) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2], geom_id, row[3], row[4], row[5]))

		conn.commit()

		break

	print "Now run command 'radon_tables.py -r %s' to update as_grid" % (prod_id)

def producer_id(grb):
	conn = get_conn()
	cur = conn.cursor()

	centre = grib_get(grb, "centre", int)
	ident = grib_get(grb, "generatingProcessIdentifier")

	query = "SELECT producer_id FROM producer_grib WHERE centre = %s AND ident = %s"

	cur.execute(query, (centre, ident))
	row = cur.fetchone()

	if row is None:	
		return None

	return int(row[0])

def grid_span(grb):
	di = grib_get(grb, "iDirectionIncrementInDegrees")
	dj = grib_get(grb, "jDirectionIncrementInDegrees")

	return di,dj

def grid_span_meters(grb):
	di = int(grib_get(grb, "DxInMetres"))
	dj = int(grib_get(grb, "DyInMetres"))

	return di,dj

def grid_size(grb):
	ni = int(grib_get(grb, "Ni"))
	nj = int(grib_get(grb, "Nj"))

	return ni,nj

def scanning_mode(grb):
	ineg = grib_get(grb, "iScansNegatively")
	jpos = grib_get(grb, "jScansPositively")

	if not ineg and jpos:
		return "+x+y"
	elif not ineg and not jpos:
		return "+x-y"
	else:
		print "unsupported scanning mode"
		sys.exit(1)

def first_point(grb):
	first_x = grib_get(grb, "longitudeOfFirstGridPointInDegrees")
	first_y = grib_get(grb, "latitudeOfFirstGridPointInDegrees")

	return first_x,first_y

def ask_for_name_and_desc():
	name = raw_input("Give geometry name (return for abort): ")

	if len(name) == 0:
		sys.exit(1)

	desc = raw_input("Give geometry description (return for abort): ")
	
	if len(desc) == 0:
		sys.exit(1)

	print "Name: %s\nDescription: %s" % (name, desc)

	return name,desc

def ll(grb):
	lon,lat = first_point(grb)
	scmode = scanning_mode(grb)
	ni,nj = grid_size(grb)
	di,dj = grid_span(grb)

	conn = get_conn()
	cur = conn.cursor()

	query = "SELECT id, name FROM geom_latitude_longitude WHERE st_x(first_point) = %s AND st_y(first_point) = %s AND ni = %s AND nj = %s AND di = %s AND dj = %s"

	cur.execute(query, (lon, lat, ni, nj, di, dj))
	row = cur.fetchone()

	if row is not None:
		print "Geometry already exists (%s %s)" % (row[0], row[1])
		return

	print "First point: %s,%s\nNi: %d\nNj: %d\nDi: %s\nDj: %s\nScanning mode: %s" % (lon, lat, ni, nj, di, dj, scmode)

	name,desc = ask_for_name_and_desc()

	proceed = raw_input("Proceed [y/n] (return for abort)? ")

	if len(proceed) == 0:
		sys.exit(1)
	elif proceed != "y":
		return

	query = "INSERT INTO geom (id, name, projection_id) VALUES (DEFAULT, %s, 1) RETURNING ID"

	cur.execute(query, (name,))

	geom_id = int(cur.fetchone()[0])

	query = "INSERT INTO geom_latitude_longitude (id, name, ni, nj, first_point, di, dj, scanning_mode, description) VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s)"

	#print cur.mogrify(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc))
	cur.execute(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc))

	conn.commit()

	print "Geometry inserted: %s %s" % (geom_id, name)

	update_table_meta_grid(grb, geom_id, name)

def rll(grb):
	lon,lat = first_point(grb)
	scmode = scanning_mode(grb)
	ni,nj = grid_size(grb)
	di,dj = grid_span(grb)

	splon = grib_get(grb, "longitudeOfSouthernPoleInDegrees")
	splat = grib_get(grb, "latitudeOfSouthernPoleInDegrees")

	conn = get_conn()
	cur = conn.cursor()

	query = "SELECT id, name FROM geom_rotated_latitude_longitude WHERE st_x(first_point) = %s AND st_y(first_point) = %s AND ni = %s AND nj = %s AND di = %s AND dj = %s AND st_x(south_pole) = %s AND st_y(south_pole) = %s"

#	print cur.mogrify(query, (lon, lat, ni, nj, di, dj, splon, splat))
	cur.execute(query, (lon, lat, ni, nj, di, dj, splon, splat))
	row = cur.fetchone()

	if row is not None:
		print "Geometry already exists (%s %s)" % (row[0], row[1])
		return

	print "First point: %s,%s\nNi: %d\nNj: %d\nDi: %s\nDj: %s\nScanning mode: %s\nSouth pole: %s,%s" % (lon, lat, ni, nj, di, dj, scmode, splon, splat)

	name,desc = ask_for_name_and_desc()

	proceed = raw_input("Proceed [y/n] (return for abort)? ")

	if len(proceed) == 0:
		sys.exit(1)
	elif proceed != "y":
		return

	query = "INSERT INTO geom (id, name, projection_id) VALUES (DEFAULT, %s, 4) RETURNING ID"

	cur.execute(query, (name,))

	geom_id = int(cur.fetchone()[0])

	query = "INSERT INTO geom_rotated_latitude_longitude (id, name, ni, nj, first_point, di, dj, scanning_mode, description, south_pole) VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))"

	#print cur.mogrify(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc))
	cur.execute(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc, splon, splat))

	conn.commit()

	print "Geometry inserted: %s %s" % (geom_id, name)

	update_table_meta_grid(grb, geom_id, name)


def ps(grb):
	lon,lat = first_point(grb)
	scmode = scanning_mode(grb)
	ni,nj = grid_size(grb)
	di,dj = grid_span_meters(grb)

	orient = grib_get(grb, "orientationOfTheGridInDegrees")

	conn = get_conn()
	cur = conn.cursor()

	query = "SELECT id, name FROM geom_stereographic WHERE st_x(first_point) = %s AND st_y(first_point) = %s AND ni = %s AND nj = %s AND di = %s AND dj = %s AND orientation = %s"

	cur.execute(query, (lon, lat, ni, nj, di, dj, orient))
	row = cur.fetchone()

	if row is not None:
		print "Geometry already exists (%s %s)" % (row[0], row[1])
		return

	print "First point: %s,%s\nNi: %d\nNj: %d\nDi: %s\nDj: %s\nScanning mode: %s\nOrientation: %s" % (lon, lat, ni, nj, di, dj, scmode, orient)

	name,desc = ask_for_name_and_desc()

	proceed = raw_input("Proceed [y/n] (return for abort)? ")

	if len(proceed) == 0:
		sys.exit(1)
	elif proceed != "y":
		return

	query = "INSERT INTO geom (id, name, projection_id) VALUES (DEFAULT, %s, 2) RETURNING ID"

	cur.execute(query, (name,))

	geom_id = int(cur.fetchone()[0])

	query = "INSERT INTO geom_stereographic (id, name, ni, nj, first_point, di, dj, scanning_mode, description, orientation) VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s)"

	#print cur.mogrify(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc))
	cur.execute(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc, orient))

	conn.commit()

	print "Geometry inserted: %s %s" % (geom_id, name)

	update_table_meta_grid(grb, geom_id, name)

def lcc(grb):
	lon,lat = first_point(grb)
	scmode = scanning_mode(grb)
	ni,nj = grid_size(grb)
	di,dj = grid_span_meters(grb)

	orient = grib_get(grb, "orientationOfTheGridInDegrees")
	latin1 = grib_get(grb, "Latin1InDegrees")
	latin2 = grib_get(grb, "Latin2InDegrees")
	splon = grib_get(grb, "longitudeOfSouthernPoleInDegrees")
	splat = grib_get(grb, "latitudeOfSouthernPoleInDegrees")

	conn = get_conn()
	cur = conn.cursor()

	query = "SELECT id, name FROM geom_lambert_conformal WHERE st_x(first_point) = %s AND st_y(first_point) = %s AND ni = %s AND nj = %s AND di = %s AND dj = %s AND orientation = %s and latin1 = %s and latin2 = %s and st_x(south_pole) = %s AND st_y(south_pole) = %s "

	cur.execute(query, (lon, lat, ni, nj, di, dj, orient, latin1, latin2, splon, splat))
	row = cur.fetchone()

	if row is not None:
		print "Geometry already exists (%s %s)" % (row[0], row[1])
		return

	print "First point: %s,%s\nNi: %d\nNj: %d\nDi: %s\nDj: %s\nScanning mode: %s\nOrientation: %s\nLatin1: %s\nLatin2: %s\nSouth pole: %s,%s\n" % (lon, lat, ni, nj, di, dj, scmode, orient, latin1, latin2, splon, splat)

	name,desc = ask_for_name_and_desc()

	proceed = raw_input("Proceed [y/n] (return for abort)? ")

	if len(proceed) == 0:
		sys.exit(1)
	elif proceed != "y":
		return

	query = "INSERT INTO geom (id, name, projection_id) VALUES (DEFAULT, %s, 2) RETURNING ID"

	cur.execute(query, (name,))

	geom_id = int(cur.fetchone()[0])

	query = "INSERT INTO geom_lambert_conformal (id, name, ni, nj, first_point, di, dj, scanning_mode, description, orientation, latin1, latin2, south_pole) VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))"

	#print cur.mogrify(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc))
	cur.execute(query, (geom_id, name, ni, nj, lon, lat, di, dj, scmode, desc, orient, latin1, latin2, splon, splat))

	conn.commit()

	print "Geometry inserted: %s %s" % (geom_id, name)

	update_table_meta_grid(grb, geom_id, name)

def main(filename):
	print "%s" % (filename)

	grbfile = open(filename)

	while True:
		grb = grib_new_from_file(grbfile)

		if grb is None:
			break

		grid_type = grib_get(grb, "gridType")

		if grid_type == "regular_ll":
			ll(grb)
		elif grid_type == "rotated_ll":
			rll(grb)
		elif grid_type == "polar_stereographic":
			ps(grb)
		elif grid_type == "lambert":
			lcc(grb)
		else:
			print "unsupported grid_type: %s" % (grid_type)


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "usage: %s gribfile" % (sys.argv[0])
		print "interactive tool to insert geometry information from grib to radon database"
		sys.exit()

	main(sys.argv[1])
