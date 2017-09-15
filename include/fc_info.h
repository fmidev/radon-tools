#ifndef _FC_INFO_
#define _FC_INFO_

const float kFloatMissing = 32700.f;

struct fc_info
{
	fc_info()
	    // COMMON FOR RADON AND NEONS
	    : ednum(0),
	      centre(0),
	      discipline(0),
	      category(0),
	      process(0),
	      year(0),
	      month(0),
	      day(0),
	      hour(0),
	      minute(0),
	      stepType(0),
	      startstep(0),
	      endstep(0),
	      timeUnit(0),
	      ni(0),
	      nj(0),
	      lat(kFloatMissing),
	      lon(kFloatMissing),
	      di(kFloatMissing),
	      dj(kFloatMissing),
	      step(0),
	      param(0),
	      novers(0),
	      levtype(0),
	      level1(0),
	      level2(0),
	      lvl1_lvl2(0),
	      fcst_per(0),
	      timeRangeIndicator(0),
	      base_date(""),
	      base_date_sql(""),
	      parname(""),
	      levname(""),
	      grtyp(""),
	      filetype(""),
	      filename(""),
	      ncname("")
	      // RADON ONLY
	      ,
	      forecast_type_id(1)  // deterministic
	      ,
	      forecast_type_value(-1),
	      gridtype(0),
	      lat_degrees(kFloatMissing),
	      lon_degrees(kFloatMissing),
	      di_degrees(kFloatMissing),
	      dj_degrees(kFloatMissing),
	      di_meters(kFloatMissing),
	      dj_meters(kFloatMissing){};

	// edition 1 = grib1, 2 = grib2, 3 = netcdf
	long ednum;
	long centre;
	long discipline;
	long category;
	long process;
	long year;
	long month;
	long day;
	long hour;
	long minute;
	long stepType;
	long startstep;
	long endstep;
	long timeUnit;
	long ni;
	long nj;
	double lat;
	double lon;
	double di;
	double dj;
	long step;
	long param;
	long novers;
	long levtype;
	long level1;
	long level2;
	long lvl1_lvl2;
	long fcst_per;
	long timeRangeIndicator;
	std::string base_date;
	std::string base_date_sql;
	std::string parname;
	std::string levname;
	std::string grtyp;
	std::string filetype;
	std::string filename;
	std::string ncname;
	long forecast_type_id;  // from table forecast_type
	double forecast_type_value;
	long gridtype;
	double lat_degrees;
	double lon_degrees;
	double di_degrees;
	double dj_degrees;
	double di_meters;
	double dj_meters;
};

#endif
