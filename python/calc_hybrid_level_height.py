#!/usr/bin/env python3

import psycopg2
import eccodes as ecc
import os
import sys
import statistics

conn = None
cur = None

params = {
  'P-HPA' : 412,
  'HL-M'  : 421
}

def get_forecast_type_id(producer_id):
    if producer_id == 243 or producer_id == 260:
        return 4
    return 1

def insert_to_radon(producer_id, geometry_name, analysis_time, data):

    press = data[412]
    heigh = data[421]
    for i,level in enumerate(press):
        p = press[level]
        h = heigh[level]

        minp = round(min(p['min']), 1)
        minp_stdev = round(statistics.stdev(p['min']), 1)
        maxp = round(max(p['max']), 1)
        maxp_stdev = round(statistics.stdev(p['max']), 1)
        minh = round(min(h['min']), 1)
        minh_stdev = round(statistics.stdev(h['min']), 1)
        maxh = round(min(h['max']), 1)
        maxh_stdev = round(statistics.stdev(h['max']), 1)
        aveh = round(statistics.mean(h['ave']), 1)
        avep = round(statistics.mean(p['ave']), 1)

        sql = "INSERT INTO hybrid_level_height AS h (producer_id, geometry_id, level_value, analysis_time, minimum_height, minimum_height_stddev, minimum_pressure, minimum_pressure_stddev, maximum_height, maximum_height_stddev, maximum_pressure, maximum_pressure_stddev, average_height, average_pressure, count) VALUES (%s, (SELECT id FROM geom WHERE name = %s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (producer_id, geometry_id, level_value) DO UPDATE SET analysis_time = EXCLUDED.analysis_time, minimum_height = EXCLUDED.minimum_height, minimum_height_stddev = EXCLUDED.minimum_height_stddev, minimum_pressure = EXCLUDED.minimum_pressure, minimum_pressure_stddev = EXCLUDED.minimum_pressure_stddev, maximum_height = EXCLUDED.maximum_height, maximum_height_stddev = EXCLUDED.maximum_height_stddev, maximum_pressure = EXCLUDED.maximum_pressure, maximum_pressure_stddev = EXCLUDED.maximum_pressure_stddev, average_height = EXCLUDED.average_height, average_pressure = EXCLUDED.average_pressure, count = EXCLUDED.count WHERE h.producer_id = %s AND h.geometry_id = (SELECT id FROM geom WHERE name = %s) AND h.level_value = %s"
        cur.execute(sql, (producer_id, geometry_name, level, analysis_time, minh, minh_stdev, minp, minp_stdev, maxh, maxh_stdev, maxp, maxp_stdev, aveh, avep, len(p['min']), producer_id, geometry_name, level))

    conn.commit()


def read_data(files):
    ret = { }
    for i in list(params.values()):
        ret[i] = {}

    print("level: ", end="")

    for f in files:
        i = i + 1
        param = f[0]
        fname = f[1]
        offset = f[3]
        length = f[4]
        level = f[5]
        print(level,fname,offset,length)
        with open(fname, "rb") as fp:
            fp.seek(offset, 0)
            buff = fp.read(length)
            fp.close()

        gh = ecc.codes_new_from_message(buff)
        minv = ecc.codes_get(gh, 'minimum')
        maxv = ecc.codes_get(gh, 'maximum')
        ave = ecc.codes_get(gh, 'average')

        try:
            ret[param][level]['max'].append(maxv)
            ret[param][level]['min'].append(minv)
            ret[param][level]['ave'].append(ave)

        except KeyError as e:
            ret[param][level] = {}
            ret[param][level]['max'] = [maxv]
            ret[param][level]['min'] = [minv]
            ret[param][level]['ave'] = [ave]

        ecc.codes_release(gh)

    return ret

def read_files(producer_id, geometry_name):
    sql = "SELECT schema_name, partition_name, analysis_time FROM as_grid_v WHERE producer_id = %s AND record_count = 1 AND geometry_name = %s ORDER BY analysis_time LIMIT 1"
    cur.execute(sql, (producer_id, geometry_name))
    row = cur.fetchone()
    analysis_time = row[2]

    sql = f"SELECT param_id,file_location,message_no,byte_offset,byte_length,level_value FROM {row[0]}.{row[1]} WHERE forecast_type_id = %s AND forecast_period <= '24:00:00' AND param_id IN %s AND level_id = 3 AND geometry_id = (SELECT id FROM geom WHERE name = %s) ORDER by param_id, level_value"
    cur.execute(sql, (get_forecast_type_id(producer_id),tuple(params.values()), geometry_name))
    rows = cur.fetchall()
    return (analysis_time, rows)


def connect():
    global conn
    global cur

    conn = psycopg2.connect("host={} dbname=radon user=wetodb password={}".format(os.environ['RADON_HOSTNAME'], os.environ['RADON_WETODB_PASSWORD']))
    cur = conn.cursor()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} PRODUCER_ID GEOMETRY_NAME")
        sys.exit(1)

    producer_id = int(sys.argv[1])
    geom_name = sys.argv[2]
    connect()

    print(f"producer: {producer_id} geometry: {geom_name}")
    analysis_time, files = read_files(producer_id, geom_name)
    data = read_data(files)
    insert_to_radon(producer_id, geom_name, analysis_time, data)
    print("Finished")
