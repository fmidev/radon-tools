#ifndef _FC_INFO_
#define _FC_INFO_

struct fc_info 
{
	fc_info() 
	{ 
		// initialization missing !
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
	std::string hostname;
	// neon2-only variables
	long producer_id; // from table fmi_producer
	long param_id; // from table param_{grib1|grib2|netcdf}
	long level_id; // from table level_{grib1|grib2}
	double level_value;
	long forecast_type_id; // from table forecast_type
	long projection_id; // from table projections
	long grid_type; // grid type id as gotten from grib
};

#endif
