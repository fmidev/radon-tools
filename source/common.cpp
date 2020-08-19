#include "common.h"
#include "options.h"
#include <boost/regex.hpp>
#include <plugin_factory.h>
#include <sstream>
#include <util.h>

#define HIMAN_AUXILIARY_INCLUDE
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;
std::mutex dirCreateMutex, ssMutex;

std::string grid_to_radon::common::CanonicalFileName(const std::string& inputFileName)
{
	auto theFileName = inputFileName;
	namespace fs = boost::filesystem;

	fs::path pathname(theFileName);
	pathname = canonical(fs::system_complete(pathname));

	if (options.directory_structure_check && !CheckDirectoryStructure(pathname))
	{
		throw std::runtime_error("Directory structure check failed");
	}

	std::string dirName = pathname.parent_path().string();

	return dirName + "/" + pathname.filename().string();
}

std::string grid_to_radon::common::MakeFileName(std::shared_ptr<himan::configuration>& config,
                                                std::shared_ptr<himan::info<double>>& info,
                                                const std::string& inputFileName)
{
	if (options.in_place_insert)
	{
		return CanonicalFileName(inputFileName);
	}
	else
	{
		himan::plugin_configuration pconfig(*config);
		return himan::util::MakeFileName(*info, pconfig);
	}
}

bool grid_to_radon::common::CheckDirectoryStructure(const boost::filesystem::path& pathname)
{
	// Check that directory is in the form: /path/to/some/directory/<yyyymmddhh24mi>/<producer_id>/
	const auto atimedir = pathname.parent_path().filename();
	const auto proddir = pathname.parent_path().parent_path().filename();

	const boost::regex r1("\\d+");
	const boost::regex r2("\\d{12}");

	if (boost::regex_match(proddir.string(), r1) == false || boost::regex_match(atimedir.string(), r2) == false)
	{
		himan::logger logr("common");
		logr.Error(
		    "File path must include analysistime and producer id "
		    "('/path/to/some/dir/<producer_id>/<analysistime12digits>/file.grib', got file '" +
		    pathname.string() + "'");
		return false;
	}
	return true;
}

void grid_to_radon::common::CreateDirectory(const std::string& theFileName)
{
	namespace fs = boost::filesystem;

	fs::path pathname(theFileName);

	if (!fs::is_directory(pathname.parent_path()))
	{
		std::lock_guard<std::mutex> lock(dirCreateMutex);

		if (!fs::is_directory(pathname.parent_path()))
		{
			fs::create_directories(pathname.parent_path());
		}
	}
}

bool grid_to_radon::common::SaveToDatabase(std::shared_ptr<himan::configuration>& config,
                                           std::shared_ptr<himan::info<double>>& info,
                                           std::shared_ptr<himan::plugin::radon>& r,
                                           const himan::file_information& finfo,
                                           std::set<std::string>& ssStateInformation)
{
	if (!options.dry_run)
	{
		if (!r->Save<double>(*info, finfo, ""))
		{
			return false;
		}

		if (options.ss_state_update)
		{
			SaveSSStateInformation(config, info, ssStateInformation, r);
		}
	}
	return true;
}

void grid_to_radon::common::SaveSSStateInformation(std::shared_ptr<himan::configuration>& config,
                                                   std::shared_ptr<himan::info<double>>& info,
                                                   std::set<std::string>& ssStateInformation,
                                                   std::shared_ptr<himan::plugin::radon>& r)
{
	const auto base_date = info->Time().OriginDateTime().String("%Y-%m-%d %H:%M:%S");

	std::string tableName;

	if (options.ss_table_name.empty() == false)
	{
		tableName = options.ss_table_name;
	}
	else
	{
		auto tableinfo = r->RadonDB().GetTableName(info->Producer().Id(), base_date, config->TargetGeomName());
		tableName = tableinfo["schema_name"] + "." + tableinfo["table_name"];
	}

	std::stringstream ss;
	ss << info->Producer().Id() << "/" << config->TargetGeomName() << "/" << info->Time().OriginDateTime().String()
	   << "/" << info->Time().Step().String("%h:%02M:%02S") << "/" << static_cast<int> (info->ForecastType().Type()) << "/"
	   << info->ForecastType().Value() << "/" << tableName;

	{
		std::lock_guard<std::mutex> lock(ssMutex);
		ssStateInformation.insert(ss.str());
	}
}

void grid_to_radon::common::UpdateSSState(const std::set<std::string>& ssStateInformation)
{
	if (!options.ss_state_update)
	{
		return;
	}

	auto ldr = GET_PLUGIN(radon);

	himan::logger logr("common");

	for (const std::string& ssInfo : ssStateInformation)
	{
		std::vector<std::string> tokens = himan::util::Split(ssInfo, "/");

		std::stringstream ss;
		ss << "INSERT INTO ss_state (producer_id, geometry_id, analysis_time, forecast_period, forecast_type_id, "
		      "forecast_type_value, table_name) VALUES ("
		   << tokens[0] << ", (SELECT id FROM geom WHERE name = '" << tokens[1] << "'), "
		   << "'" << tokens[2] << "', '" << tokens[3] << "'::interval, " << tokens[4] << ", " << tokens[5] << ", "
		   << "'" << tokens[6] << "')";

		logr.Debug("Updating ss_state for " + ssInfo);

		if (options.dry_run)
		{
			continue;
		}

		try
		{
			ldr->RadonDB().Execute(ss.str());
		}
		catch (const pqxx::unique_violation& e)
		{
			try
			{
				ss.str("");
				ss << "UPDATE ss_state SET last_updated = now(), table_name = '" + tokens[6] + "' WHERE "
				   << "producer_id = " << tokens[0] << " AND "
				   << "geometry_id = (SELECT id FROM geom WHERE name = '" << tokens[1] << "') AND "
				   << "analysis_time = '" << tokens[2] << "' AND "
				   << "forecast_period = '" << tokens[3] << "' AND "
				   << "forecast_type_id = " << tokens[4] << " AND "
				   << "forecast_type_value = " << tokens[5];

				ldr->RadonDB().Execute(ss.str());
			}
			catch (const pqxx::pqxx_exception& ee)
			{
				logr.Error("Updating ss_state information failed: " + std::string(e.what()));
			}
		}
	}
}
