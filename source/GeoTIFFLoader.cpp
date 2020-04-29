#include "GeoTIFFLoader.h"
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "cpl_conv.h"
#include "fc_info.h"
#include "options.h"
#include <algorithm>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <ctime>
#include <iomanip>
#include <ogr_spatialref.h>
#include <sstream>
#include <stdexcept>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#include "gdal_priv.h"

#pragma GCC diagnostic pop

extern Options options;

using namespace std;

static atomic<int> g_failedParams(0);
static atomic<int> g_succeededParams(0);

namespace
{
bool GetGeometryInformation(BDAPLoader& databaseLoader, fc_info& info)
{
	double di = info.di_degrees;
	double dj = info.dj_degrees;

	if (info.projection == "polster" || info.projection == "laea" || info.projection == "tm")
	{
		di = info.di_meters;
		dj = info.dj_meters;
	}

	auto geominfo = databaseLoader.RadonDB().GetGeometryDefinition(info.ni, info.nj, info.lat_degrees, info.lon_degrees,
	                                                               di, dj, 2, static_cast<int>(info.gridtype));

	if (geominfo.empty())
	{
		cerr << "Geometry not found from radon: " << info.ni << " " << info.nj << " " << info.lat_degrees << " "
		     << info.lon_degrees << " " << di << " " << dj << " " << info.gridtype << endl;
		return false;
	}

	info.geom_id = stol(geominfo["id"]);
	info.geom_name = geominfo["name"];

	return true;
}
}

void SQLTimeMaskToCTimeMask(std::string& sqlTimeMask)
{
	boost::replace_all(sqlTimeMask, "YYYY", "%Y");
	boost::replace_all(sqlTimeMask, "MM", "%m");
	boost::replace_all(sqlTimeMask, "DD", "%d");
	boost::replace_all(sqlTimeMask, "hh", "%H");
	boost::replace_all(sqlTimeMask, "mm", "%M");
}

void ParseTimeFromString(const std::string& analysistime, const std::string& mask, fc_info& info)
{
	info.year = boost::lexical_cast<int>(analysistime.substr(0, 4));
	info.month = boost::lexical_cast<int>(analysistime.substr(4, 2));
	info.day = boost::lexical_cast<int>(analysistime.substr(6, 2));
	info.hour = boost::lexical_cast<int>(analysistime.substr(8, 2));
	info.minute = 0;

	if (analysistime.length() > 10)
	{
		info.minute = boost::lexical_cast<int>(analysistime.substr(10, 2));
	}
}

GeoTIFFLoader::GeoTIFFLoader()
{
	char myhost[128];
	gethostname(myhost, 128);
	itsHostName = string(myhost);
}

void ReadAreaAndGrid(GDALDataset* poDataset, fc_info& info)
{
	info.ni = poDataset->GetRasterXSize();
	info.nj = poDataset->GetRasterYSize();

	double adfGeoTransform[6];
	if (poDataset->GetGeoTransform(adfGeoTransform) != CE_None)
	{
		throw std::runtime_error("File does not contain geo transformation coefficients");
	}

	std::string proj = poDataset->GetProjectionRef();

	if (proj.empty() == false)
	{
		OGRSpatialReference spRef(proj.c_str());
		const std::string projection = spRef.GetAttrValue("PROJECTION");

		double x0 = adfGeoTransform[0], y0 = adfGeoTransform[3];
		OGRSpatialReference* geogCS = spRef.CloneGeogCS();
		OGRCoordinateTransformation* xform = OGRCreateCoordinateTransformation(&spRef, geogCS);

		if (!xform->Transform(1, &x0, &y0))
		{
			throw std::runtime_error("Failed to get lonlat of first point");
		}

		delete xform;
		delete geogCS;

		if (projection == SRS_PT_LAMBERT_AZIMUTHAL_EQUAL_AREA)
		{
			info.gridtype = 140;
			info.projection = "laea";
			info.di_meters = floor(adfGeoTransform[1] * 10000) / 10000;
			info.dj_meters = floor(fabs(adfGeoTransform[5]) * 10000) / 10000;
			info.lon_degrees = floor(x0 * 100000) / 100000;
			info.lat_degrees = floor(y0 * 100000) / 100000;
		}
		else if (projection == SRS_PT_TRANSVERSE_MERCATOR)
		{
			info.gridtype = 12;
			info.projection = "tm";
			info.di_meters = floor(adfGeoTransform[1] * 10000) / 10000;
			info.dj_meters = floor(fabs(adfGeoTransform[5]) * 10000) / 10000;
			info.lon_degrees = floor(x0 * 100000) / 100000;
			info.lat_degrees = floor(y0 * 100000) / 100000;
		}
		else
		{
			throw std::runtime_error("Invalid projection: " + projection);
		}
	}
	else
	{
		info.lon_degrees = adfGeoTransform[0];
		info.lat_degrees = adfGeoTransform[3];
		info.di_degrees = floor(adfGeoTransform[1] * 10000) / 10000;
		info.dj_degrees = floor(fabs(adfGeoTransform[5]) * 10000) / 10000;
	}
}

void ReadParam(const std::map<std::string, std::string>& meta, fc_info& info, const BDAPLoader& ldr)
{
	std::string param_value;

	for (const auto& m : meta)
	{
		if (m.first == "param_name")
		{
			param_value = m.second;
			break;
		}
	}

	if (param_value.empty())
	{
		return;
	}

	auto parameter = ldr.RadonDB().GetParameterFromGeoTIFF(info.producer_id, param_value);

	auto grid_parameter_name = parameter["name"];

	if (grid_parameter_name.empty())
	{
		throw std::runtime_error("Unknown parameter: " + param_value);
	}

	info.paramid = stol(parameter["id"]);
	info.parname = grid_parameter_name;
}

void ReadLevel(const std::map<std::string, std::string>& meta, fc_info& info, const BDAPLoader& ldr)
{
	std::string level_type, level_value;

	for (const auto& m : meta)
	{
		if (m.first == "level_type")
		{
			level_type = m.second;
		}
		if (m.first == "level_value")
		{
			level_value = m.second;
		}
		if (level_type.empty() == false && level_value.empty() == false)
		{
			break;
		}
	}

	if (level_type.empty() || level_value.empty())
	{
		if (options.level.empty() == false)
		{
			info.levname = boost::to_upper_copy(options.level);
			if (info.levname == "MEANSEA")
				info.levelid = 7;
			else if (info.levname == "DEPTH")
				info.levelid = 10;
			else if (info.levname == "HEIGHT")
				info.levelid = 6;
			else if (info.levname == "PRESSURE")
				info.levelid = 2;
			else
				throw std::runtime_error("Invalid level type: " + info.levname);
		}
	}
}
void ReadForecastType(const std::map<std::string, std::string>& meta, fc_info& info, const BDAPLoader& ldr)
{
}

void ReadTime(const std::map<std::string, std::string>& meta, fc_info& info)
{
	std::string origintime, validtime, mask;
	for (const auto& m : meta)
	{
		if (m.first == "analysis_time")
			origintime = m.second;
		else if (m.first == "valid_time")
			validtime = m.second;
		else if (m.first == "time_mask")
			mask = m.second;
	}

	if (origintime.empty())
	{
		return;
	}

	ParseTimeFromString(origintime, mask, info);
}

std::map<std::string, std::string> ParseMetadata(char** mdata, BDAPLoader& databaseLoader)
{
	std::map<std::string, std::string> ret;

	std::stringstream ss;
	ss << "SELECT attribute, key, mask FROM geotiff_metadata WHERE producer_id = " << options.process;

	databaseLoader.RadonDB().Query(ss.str());

	while (true)
	{
		const auto row = databaseLoader.RadonDB().FetchRow();
		if (row.empty())
		{
			break;
		}

		const auto attribute = row[0];
		const auto keyName = row[1];
		const auto keyMask = row[2];
		//		cionst bool isRegex = (row[2] == "true") ? true : false;
		const char* m = CSLFetchNameValue(mdata, keyName.c_str());

		if (m == nullptr)
		{
			// std::cerr << "File metadata not found with key " << keyName << "\n";
			continue;
		}

		const std::string metadata = std::string(m);

		if (keyMask.empty() == false)
		{
			const boost::regex re(keyMask);
			boost::smatch what;
			if (boost::regex_search(metadata, what, re) == false || what.size() == 0)
			{
				std::cerr << "Regex:\n"
				          << keyMask << "\n============\n"
				          << "Metadata:\n"
				          << metadata << "===========\n";
				throw runtime_error("Regex did not match for attribute " + attribute);
			}

			if (what.size() != 2)
			{
				throw runtime_error("Regex matched too many times: " + std::to_string(what.size() - 1));
			}

			if (options.verbose)
				std::cout << "regex match for " << attribute << ": " << what[1] << "\n";
			ret[attribute] = what[1];
		}
		else
		{
			ret[attribute] = metadata;
		}
	}

	return ret;
}

bool GeoTIFFLoader::Load(const string& theInfile)
{
	GDALRegister_GTiff();
	GDALDataset* poDataset = reinterpret_cast<GDALDataset*>(GDALOpen(theInfile.c_str(), GA_ReadOnly));

	if (poDataset == nullptr)
	{
		return false;
	}

	auto meta = ParseMetadata(poDataset->GetMetadata(), itsDatabaseLoader);
	if (meta.size() == 0)
	{
		std::cerr << "Failed to parse metadata from " << theInfile << endl;
		return false;
	}

	fc_info info;

	if (options.process == 0)
	{
		cerr << "process value not found" << endl;
		return false;
	}

	if (options.analysistime.empty() == false)
	{
		if (options.analysistime.size() < 10)
		{
			cerr << "Invalid format for analysistime: " << options.analysistime << endl;
			cerr << "Use YYYYMMDDHH24[MI]" << endl;
			return false;
		}

		ParseTimeFromString(options.analysistime, "YYYYMMDDHH24", info);
	}
	else
	{
		ReadTime(meta, info);
	}

	info.producer_id = options.process;
	ReadAreaAndGrid(poDataset, info);
	GetGeometryInformation(itsDatabaseLoader, info);
	ReadParam(meta, info, itsDatabaseLoader);
	ReadLevel(meta, info, itsDatabaseLoader);
	ReadForecastType(meta, info, itsDatabaseLoader);

	info.fileprotocol = 1;  // local file
	info.filehost = itsHostName;

	info.ednum = 5;  // 5 --> geotiff
	info.level2 = -1;
	info.timeUnit = 1;  // hour

	set<string> analyzeTables;

	info.filename = itsDatabaseLoader.REFFileName(info);

	namespace fs = boost::filesystem;

	fs::path pathname(theInfile);
	pathname = canonical(fs::system_complete(pathname));

	if (options.directory_structure_check)
	{
		// Check that directory is in the form: /path/to/some/directory/<yyyymmddhh24mi>/<producer_id>/
		const auto atimedir = pathname.parent_path().filename();
		const auto proddir = pathname.parent_path().parent_path().filename();

		const boost::regex r1("\\d+");
		const boost::regex r2("\\d{12}");

		if (boost::regex_match(proddir.string(), r1) == false || boost::regex_match(atimedir.string(), r2) == false)
		{
			printf(
			    "File path must include analysistime and producer id "
			    "('/path/to/some/dir/<producer_id>/<analysistime12digits>/file.grib')\n");
			printf("Got file '%s'\n", pathname.string().c_str());
			return false;
		}
	}

	string dirName = pathname.parent_path().string();

	// Input file name can contain path, or not.
	// Force full path to the file even if user has not given it

	info.filename = dirName + "/" + pathname.filename().string();

	const int NUM_BANDS = poDataset->GetRasterCount();
	for (int i = 1; i <= NUM_BANDS; i++)
	{
		fc_info band_info = info;

		auto band = poDataset->GetRasterBand(i);
		auto band_meta = ParseMetadata(band->GetMetadata(), itsDatabaseLoader);

		// raster ban may contain its own metadata
		ReadParam(band_meta, band_info, itsDatabaseLoader);
		ReadLevel(band_meta, band_info, itsDatabaseLoader);
		ReadTime(band_meta, band_info);
		ReadForecastType(band_meta, band_info, itsDatabaseLoader);

		band_info.messageNo = i;

		if (!options.dry_run)
		{
			if (!itsDatabaseLoader.WriteToRadon(band_info))
			{
				g_failedParams++;
				continue;
			}

			if (itsDatabaseLoader.NeedsAnalyze())
			{
				const auto table = itsDatabaseLoader.LastInsertedTable();
				std::stringstream ss;
				ss << band_info.year << setw(2) << setfill('0') << band_info.month << setw(2) << setfill('0')
				   << band_info.day << setw(2) << setfill('0') << band_info.hour << setw(2) << setfill('0')
				   << band_info.minute << "00";
				analyzeTables.insert(table + "." + ss.str());
			}

			if (options.verbose)
			{
				cout << "Wrote param " << band_info.parname << " from band " << i << " of file '" << band_info.filename
				     << "'" << endl;
			}

			g_succeededParams++;
		}
	}

	cout << "Success with " << g_succeededParams << " params, "
	     << "failed with " << g_failedParams << " params" << endl;

	stringstream ss;

	for (const auto& table : analyzeTables)
	{
		std::vector<std::string> tokens;
		boost::split(tokens, table, boost::is_any_of("."));

		assert(tokens.size() == 3);

		ss.str("");
		ss << "UPDATE as_grid SET record_count = 1 WHERE schema_name = '" << tokens[0] << "' AND partition_name = '"
		   << tokens[1] << "' AND analysis_time = to_timestamp('" << tokens[2] << "', 'yyyymmddhh24miss')";

		if (options.verbose)
		{
			cout << "Updating record_count" << endl;
		}

		if (options.dry_run)
		{
			cout << ss.str() << endl;
		}
		else
		{
			itsDatabaseLoader.RadonDB().Execute(ss.str());
		}
	}

	// We need to check for 'total failure' if the user didn't specify a max_failures value.
	if (options.max_failures == -1 && options.max_skipped == -1)
	{
		if (g_succeededParams == 0)
		{
			return false;
		}
	}

	return true;
}
