#pragma once

#include "forecast_time.h"
#include "forecast_type.h"
#include "level.h"
#include "param.h"
#include "producer.h"

namespace grid_to_radon
{
struct record
{
	std::string schema_name;
	std::string table_name;
	std::string file_name;
	himan::HPFileType file_type;
	std::string geometry_name;
	int geometry_id;
	himan::producer producer;
	himan::forecast_type ftype;
	himan::forecast_time ftime;
	himan::level level;
	himan::param param;
	record() = default;
	record(const std::string schema_name_, const std::string& table_name_, const std::string& file_name_, himan::HPFileType file_type_,
	       const std::string& geometry_name_, int geometry_id_, const himan::producer& producer_, const himan::forecast_type ftype_,
	       const himan::forecast_time ftime_, const himan::level level_, const himan::param param_)
	    : schema_name(schema_name_),table_name(table_name_),
	      file_name(file_name_),
	      file_type(file_type_),
	      geometry_name(geometry_name_),
              geometry_id(geometry_id_),
	      producer(producer_),
	      ftype(ftype_),
	      ftime(ftime_),
	      level(level_),
	      param(param_)
	{
	}
};

typedef std::vector<record> records;
}
