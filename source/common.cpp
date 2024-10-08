#include "common.h"
#include "filename.h"
#include "options.h"
#include "util.h"
#include <filesystem>
#include <plugin_factory.h>
#include <regex>
#include <sstream>

#define HIMAN_AUXILIARY_INCLUDE
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;
std::mutex dirCreateMutex;

bool CheckDirectoryStructure(const std::filesystem::path& pathname);

std::string grid_to_radon::common::StripProtocol(const std::string& str)
{
	const static std::regex r("^(https)|(http)|(s3)*://");

	return regex_replace(str, r, "");
}

bool grid_to_radon::common::CheckForFailure(int g_failed, int g_skipped, int g_success)
{
	bool retval = true;

	if (options.max_failures >= 0 && g_failed > options.max_failures)
	{
		retval = false;
	}

	if (options.max_skipped >= 0 && g_skipped > options.max_skipped)
	{
		retval = false;
	}

	// We need to check for 'total failure' if the user didn't specify a max_failures value.
	if (options.max_failures == -1 && options.max_skipped == -1)
	{
		if (g_success == 0)
		{
			retval = false;
		}
	}

	return retval;
}

std::string grid_to_radon::common::CanonicalFileName(const std::string& inputFileName)
{
	auto theFileName = inputFileName;
	namespace fs = std::filesystem;

	fs::path pathname(theFileName);
	pathname = fs::canonical(fs::absolute(pathname));

	std::string dirName = pathname.parent_path().string();

	return dirName + "/" + pathname.filename().string();
}

std::string grid_to_radon::common::MakeFileName(std::shared_ptr<himan::configuration>& config,
                                                std::shared_ptr<himan::info<double>>& info,
                                                const std::string& inputFileName)
{
	if (options.s3)
	{
		return inputFileName;
	}

	if (options.in_place_insert)
	{
		return CanonicalFileName(inputFileName);
	}

	himan::plugin_configuration pconfig(*config);
	return himan::util::filename::MakeFileName(*info, pconfig);
}

void grid_to_radon::common::CreateDirectory(const std::string& theFileName)
{
	namespace fs = std::filesystem;

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

grid_to_radon::record Merge(std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
                            const himan::file_information& finfo, const himan::plugin::radon_record& rrec)
{
	return grid_to_radon::record(rrec.schema_name, rrec.table_name, finfo.file_location, config->OutputFileType(),
	                             rrec.geometry_name, rrec.geometry_id, info->Producer(), info->ForecastType(),
	                             info->Time(), info->Level(), info->Param());
}

std::pair<bool, grid_to_radon::record> grid_to_radon::common::SaveToDatabase(
    std::shared_ptr<himan::configuration>& config, std::shared_ptr<himan::info<double>>& info,
    std::shared_ptr<himan::plugin::radon>& r, const himan::file_information& finfo)
{
	auto ret = r->Save<double>(*info, finfo, "", options.dry_run);

	if (ret.first)
	{
		return std::make_pair(true, Merge(config, info, finfo, ret.second));
	}

	return std::make_pair(false, grid_to_radon::record());
}

void grid_to_radon::common::UpdateSSState(const grid_to_radon::records& recs)
{
	if (!options.ss_state_update)
	{
		return;
	}

	auto ldr = GET_PLUGIN(radon);

	himan::logger logr("common");

	std::set<std::string> handled;
	int skippedCount = 0;

	for (const grid_to_radon::record& rec : recs)
	{
		const std::string atime = rec.ftime.OriginDateTime().ToSQLTime();
		const std::string period = rec.ftime.Step().String("%h:%02M:%02S");

		double ftypeValue = rec.ftype.Value();

		if (ftypeValue == himan::kHPMissingValue)
		{
			ftypeValue = -1;
		}
		const std::string uniqueId = fmt::format("{}_{}_{}_{}_{}_{}", rec.producer.Id(), rec.geometry_id, atime, period,
		                                         fmt::underlying(rec.ftype.Type()), ftypeValue);

		if (std::find(handled.begin(), handled.end(), uniqueId) != handled.end())
		{
			skippedCount++;
			continue;
		}

		handled.insert(uniqueId);

		auto table_name = options.ss_table_name;

		if (table_name.empty())
		{
			table_name = fmt::format("{}.{}", rec.schema_name, rec.table_name);
		}

		std::string query = fmt::format(
		    "INSERT INTO ss_state (producer_id, geometry_id, analysis_time, forecast_period, forecast_type_id,"
		    "forecast_type_value, table_name) VALUES ({}, {}, '{}', '{}', {}, {}, '{}')",
		    rec.producer.Id(), rec.geometry_id, atime, period, fmt::underlying(rec.ftype.Type()), ftypeValue,
		    table_name);

		if (options.dry_run)
		{
			continue;
		}

		try
		{
			ldr->RadonDB().Execute(query);
		}
		catch (const pqxx::unique_violation& e)
		{
			try
			{
				query = fmt::format(
				    "UPDATE ss_state SET last_updated = now(), table_name = '{}' WHERE producer_id = {} AND "
				    "geometry_id = {} AND analysis_time = '{}' AND forecast_period = '{}' AND forecast_type_id = {} "
				    "AND forecast_type_value = {}",
				    table_name, rec.producer.Id(), rec.geometry_id, atime, period, fmt::underlying(rec.ftype.Type()),
				    ftypeValue);
				ldr->RadonDB().Execute(query);
			}
#if PQXX_VERSION_MAJOR < 7
			catch (const pqxx::pqxx_exception& ee)
			{
				logr.Error(fmt::format("Updating ss_state information failed: {}", ee.base().what()));
			}
#else
			catch (const pqxx::failure& ee)
			{
				logr.Error(fmt::format("Updating ss_state information failed: {}", ee.what()));
			}
#endif
		}
	}

	logr.Trace(fmt::format("Skipped {} duplicate ss_state entries", skippedCount));
}
