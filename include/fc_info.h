#ifndef _FC_INFO_
#define _FC_INFO_

#define kFloatMissing 32700.f

struct fc_info 
{
	fc_info()
	: grib_size(0)
	, ednum(0)
	, centre(0)
	, discipline(0)
	, category(0)
	, process(0)
	, year(0)
	, month(0)
	, day(0)
	, hour(0)
	, minute(0)
	, stepType(0)
	, startstep(0)
	, endstep(0)
	, gridtype(0)
	, timeUnit(0)
	, ni(0)
	, nj(0)
	, lat(kFloatMissing)
	, lon(kFloatMissing)
	, di(kFloatMissing)
	, dj(kFloatMissing)
	, step(0)
	, param(0)
	, novers(0)
	, levtype(0)
	, level1(0)
	, level2(0)
	, lvl1_lvl2(0)
	, locdef(0)
	, ldeftype(0)
	, ldefnumber(0)
	, fcst_per(0)
	, timeRangeIndicator(0)
	, base_date("")
	, parname("")
	, levname("")
	, grtyp("")
	, eps_specifier("")
	, filetype("")
	, filename("")
	, forecast_type_id(0)
	, forecast_type_value(kFloatMissing)
	{ 
	};

	long grib_size;
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
	long gridtype;
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
	long locdef;
	long ldeftype;
	long ldefnumber;
	long fcst_per;
	long timeRangeIndicator;
	std::string base_date;
	std::string parname;
	std::string levname;
	std::string grtyp;
	std::string eps_specifier;
	std::string filetype;
	std::string filename;
	// neon2-only variables
	long forecast_type_id; // from table forecast_type
	double forecast_type_value;
};

#endif