#ifndef _FC_INFO_
#define _FC_INFO_

const float kFloatMissing = 32700.f;

struct fc_info
{
	fc_info()
	    : ednum(0),
	      producer_id(0),
	      year(0),
	      month(0),
	      day(0),
	      hour(0),
	      minute(0),
	      timeUnit(0),
	      ni(0),
	      nj(0),
	      di(kFloatMissing),
	      dj(kFloatMissing),
	      paramid(0),
	      levelid(0),
	      level1(0),
	      level2(0),
	      fcst_per(0),
	      parname(""),
	      levname(""),
	      projection(""),
	      filename(""),
	      forecast_type_id(1),  // deterministic
	      forecast_type_value(-1),
	      gridtype(0),
	      lat_degrees(kFloatMissing),
	      lon_degrees(kFloatMissing),
	      di_degrees(kFloatMissing),
	      dj_degrees(kFloatMissing),
	      di_meters(kFloatMissing),
	      dj_meters(kFloatMissing),
	      geom_id(0),
	      geom_name(""),
	      messageNo(0),
	      offset(0),
	      length(0){};

	// edition 1 = grib1, 2 = grib2, 3 = netcdf
	long ednum;
	long producer_id;
	long year;
	long month;
	long day;
	long hour;
	long minute;
	long timeUnit;
	long ni;
	long nj;
	double di;
	double dj;
	long paramid;  // db id
	long levelid;  // db id
	long level1;
	long level2;
	long fcst_per;
	std::string parname;
	std::string levname;
	std::string projection;
	std::string filename;
	long forecast_type_id;  // from table forecast_type
	double forecast_type_value;
	long gridtype;
	double lat_degrees;
	double lon_degrees;
	double di_degrees;
	double dj_degrees;
	double di_meters;
	double dj_meters;
	long geom_id;
	std::string geom_name;
	unsigned int messageNo;
	unsigned long offset;
	unsigned long length;
};

#endif
