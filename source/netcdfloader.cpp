#include "netcdfloader.h"
#include "NFmiNetCDF.h"
#include "common.h"
#include "filename.h"
#include "info.h"
#include "lambert_conformal_grid.h"
#include "latitude_longitude_grid.h"
#include "options.h"
#include "plugin_factory.h"
#include "util.h"
#include <algorithm>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <ctime>
#include <iomanip>
#include <ogr_spatialref.h>
#include <regex>
#include <sstream>
#include <stdexcept>

#define HIMAN_AUXILIARY_INCLUDE
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;

using namespace grid_to_radon;

static std::atomic<int> g_failedParams(0);
static std::atomic<int> g_succeededParams(0);

std::map<std::string, short> pskip;

himan::raw_time StringToTime(const std::string& dateTime, const std::string& mask);

NetCDFLoader::NetCDFLoader() : itsLogger("netcdf")
{
	// StringToTime() function handles UTC only

	setenv("TZ", "UTC", 1);

	char myhost[128];
	gethostname(myhost, 128);
	itsHostName = std::string(myhost);
}

himan::raw_time ReadTime(const std::string& analysistime)
{
	std::string mask = "%Y%m%d%H";

	if (analysistime.size() > 10)
	{
		mask = "%Y%m%d%H%M";
	}

	return himan::raw_time(analysistime, mask);
}

std::unique_ptr<himan::regular_grid> ReadAreaAndGrid(NFmiNetCDF& reader)
{
	const size_t ni = reader.SizeX();
	const size_t nj = reader.SizeY();

	double lat = 0.0;
	double lon = 0.0;

	{
		NcError errorState(NcError::silent_nonfatal);

		if (reader.HasVariable("latitude") && reader.HasVariable("longitude"))
		{
			lat = reader.Lat0<double>();
			lon = reader.Lon0<double>();
		}
		else
		{
			lat = reader.Y0<double>();
			lon = reader.X0<double>();
		}
	}

	const himan::point first(lon, lat);

	const double di = reader.XResolution();
	const double dj = reader.YResolution();
	double earthRadius = 6371220.;

	try
	{
		auto projvar = reader.GetProjectionVariable();
		earthRadius = std::stod(NFmiNetCDF::Att(projvar, "earth_radius"));
	}
	catch (...)
	{
	}

	if (reader.Projection() == "latitude_longitude")
	{
		return std::unique_ptr<himan::latitude_longitude_grid>(new himan::latitude_longitude_grid(
		    himan::kBottomLeft, first, ni, nj, di, dj, himan::earth_shape<double>()));
	}
	else if (reader.Projection() == "polar_stereographic")
	{
		// fminc returns di/dj in km, radon holds meters
		return std::unique_ptr<himan::stereographic_grid>(
		    new himan::stereographic_grid(himan::kBottomLeft, first, ni, nj, 1000 * di, 1000 * dj, reader.Orientation(),
		                                  himan::earth_shape<double>(earthRadius), false));
	}
	else if (reader.Projection() == "lambert_conformal_conic")
	{
		const double stdParallel = std::stod(NFmiNetCDF::Att(reader.GetProjectionVariable(), "standard_parallel"));

		// fminc returns di/dj in km, radon holds meters
		return std::unique_ptr<himan::lambert_conformal_grid>(new himan::lambert_conformal_grid(
		    himan::kBottomLeft, first, ni, nj, 1000 * di, 1000 * dj, reader.Orientation(), stdParallel, stdParallel,
		    himan::earth_shape<double>(earthRadius), false));
	}
	else
	{
		throw std::runtime_error("Unsupported projection: " + reader.Projection());
	}
}

himan::raw_time ReadValidTime(NFmiNetCDF& reader)
{
	himan::raw_time validTime;

	switch (reader.TypeT())
	{
		case ncFloat:
			return StringToTime(std::to_string(reader.Time<float>()), reader.TimeUnit());
		case ncDouble:
			return StringToTime(std::to_string(reader.Time<double>()), reader.TimeUnit());
		case ncShort:
			return StringToTime(std::to_string(reader.Time<short>()), reader.TimeUnit());
		case ncInt:
			return StringToTime(std::to_string(reader.Time<int>()), reader.TimeUnit());
		case ncChar:
		case ncByte:
		case ncNoType:
		default:
			return himan::raw_time();
	}
}
himan::param ReadParam(NFmiNetCDF& reader, const himan::producer& prod)
{
	std::string ncname = reader.Param()->name();

	if (ncname == "latitude" || ncname == "longitude" || ncname == "time" || ncname == "x" || ncname == "y" ||
	    pskip.count(ncname) > 0)
	{
		return himan::param();
	}

	auto r = GET_PLUGIN(radon);

	std::map<std::string, std::string> parameter = r->RadonDB().GetParameterFromNetCDF(prod.Id(), ncname, -1, -1);

	himan::logger logr("netcdfloader");
	if (parameter.empty() || parameter["name"].empty())
	{
		logr.Warning("NetCDF param " + ncname + " not supported");

		g_failedParams++;
		pskip[ncname] = 1;
		return himan::param();
	}

	return himan::param(parameter["name"]);
}

std::pair<bool, records> NetCDFLoader::Load(const std::string& theInfile) const
{
	NFmiNetCDF reader;

	if (!reader.Read(theInfile))
	{
		itsLogger.Error("Unable to read file '" + theInfile + "'");
		return make_pair(false, records{});
	}

	if (options.analysistime.empty())
	{
		itsLogger.Error("Analysistime not specified");
		return make_pair(false, records{});
	}

	if (!reader.IsConvention())
	{
		itsLogger.Error("File '" + theInfile + "' is not CF conforming NetCDF");
		return make_pair(false, records{});
	}

	itsLogger.Debug("Read " + std::to_string(reader.SizeZ()) + " levels,\n" + +"     " +
	                std::to_string(reader.SizeX()) + " x coordinates,\n" + "     " + std::to_string(reader.SizeY()) +
	                " y coordinates,\n" + "     " + std::to_string(reader.SizeT()) + " timesteps,\n" + "     " +
	                std::to_string(reader.SizeParams()) + " parameters from file '" + theInfile + "'");

	/* Set struct fcinfo accordingly */

	if (options.producer == 0)
	{
		itsLogger.Error("producer_id value not found");
		return make_pair(false, records{});
	}

	const himan::producer prod(options.producer);

	if (options.analysistime.size() < 10)
	{
		itsLogger.Error("Invalid format for analysistime: " + options.analysistime);
		itsLogger.Error("Use YYYYMMDDHH24[MI]");
		return make_pair(false, records{});
	}

	const himan::raw_time originTime = ReadTime(options.analysistime);

	auto geom = ReadAreaAndGrid(reader);

	auto r = GET_PLUGIN(radon);

	auto geomdef = r->RadonDB().GetGeometryDefinition(geom->Ni(), geom->Nj(), geom->FirstPoint().Y(),
	                                                  geom->FirstPoint().X(), geom->Di(), geom->Dj(), geom->Type());

	if (geomdef.empty())
	{
		itsLogger.Error("Did not find geometry from database");
		return make_pair(false, records{});
	}

	auto config = std::make_shared<himan::configuration>();

	config->WriteToDatabase(true);
	config->WriteMode(himan::kSingleGridToAFile);
	config->DatabaseType(himan::kRadon);
	config->TargetGeomName(geomdef["name"]);
	config->OutputFileType(himan::kNetCDF);
	config->ProgramName(himan::kGridToRadon);

	auto CreateInfo = [&geom, &prod](const himan::forecast_type& ftype, const himan::forecast_time& ftime,
	                                 const himan::level& lvl,
	                                 const himan::param& par) -> std::shared_ptr<himan::info<double>>
	{
		auto info = std::make_shared<himan::info<double>>(ftype, ftime, lvl, par);
		info->Producer(prod);

		auto b = std::make_shared<himan::base<double>>();
		b->grid = std::shared_ptr<himan::grid>(geom->Clone());

		info->Create(b, false);

		// Set descriptors

		info->Find<himan::param>(par);
		info->Find<himan::forecast_time>(ftime);
		info->Find<himan::level>(lvl);
		info->Find<himan::forecast_type>(ftype);

		return info;
	};

	auto Write = [&](std::shared_ptr<himan::info<double>>& info) -> std::pair<bool, record>
	{
		const std::string theFileName = common::MakeFileName(config, info, "");

		himan::file_information finfo;
		finfo.file_type = himan::kNetCDF;
		finfo.file_location = theFileName;
		finfo.file_server = itsHostName;
		finfo.message_no = std::nullopt;
		finfo.offset = std::nullopt;
		finfo.length = std::nullopt;
		finfo.storage_type = himan::kLocalFileSystem;

		if (!options.dry_run)
		{
			if (!reader.WriteSlice(finfo.file_location))
			{
				itsLogger.Error("Write to file failed");
				return std::make_pair(false, record());
			}

			const auto ret = grid_to_radon::common::SaveToDatabase(config, info, r, finfo);

			if (!ret.first)
			{
				itsLogger.Error("Write to radon failed");
				return std::make_pair(false, record());
			}

			itsLogger.Debug("Wrote " + info->Param().Name() + " level " + static_cast<std::string>(info->Level()) +
			                " to file '" + finfo.file_location + "'");

			return std::make_pair(true, ret.second);
		}

		return std::make_pair(false, record());
	};

	const himan::forecast_type ftype(himan::kDeterministic);

	records recs;

	for (reader.ResetTime(); reader.NextTime();)
	{
		const himan::raw_time validTime = ReadValidTime(reader);

		if (validTime == himan::raw_time())
		{
			itsLogger.Warning("Unable to determine valid time from file");
			continue;
		}

		const himan::forecast_time ftime(originTime, validTime);

		itsLogger.Debug("Time " + ftime.ValidDateTime().String() + " (" + ftime.OriginDateTime().String() + " +" +
		                ftime.Step().String("%03h") + " hours)");

		reader.FirstParam();

		do
		{
			const himan::param par = ReadParam(reader, prod);

			if (par == himan::param())
			{
				continue;
			}

			itsLogger.Info("Parameter " + std::string(reader.Param()->name()) + " (" + par.Name() + ")");

			himan::level lvl;

			// Check level type

			if (options.level.empty())
			{
				// Default
				lvl = himan::level(himan::kHeight, 0);
			}
			else
			{
				lvl = himan::level(himan::HPStringToLevelType.at(boost::to_lower_copy(options.level)), 0);
			}

			if (!reader.HasDimension("z"))
			{
				// This parameter has no z dimension --> map to level 0

				auto info = CreateInfo(ftype, ftime, lvl, himan::util::InitializeParameter(prod, par, lvl));
				const auto ret = Write(info);
				if (ret.first)
				{
					recs.push_back(ret.second);
				}
			}
			else
			{
				for (reader.ResetLevel(); reader.NextLevel();)
				{
					if (options.use_level_value)
					{
						lvl.Value(reader.Level());
					}
					else if (options.use_inverse_level_value)
					{
						lvl.Value(reader.Level() * -1);
					}
					else
					{
						lvl.Value(static_cast<float>(reader.LevelIndex()));  // ordering number
					}

					auto info = CreateInfo(ftype, ftime, lvl, himan::util::InitializeParameter(prod, par, lvl));

					const auto ret = Write(info);
					if (ret.first)
					{
						recs.push_back(ret.second);
					}
				}
			}
			g_succeededParams++;
		} while (reader.NextParam());
	}
	itsLogger.Info("Success with " + std::to_string(g_succeededParams) + " params, failed with " +
	               std::to_string(g_failedParams) + " params");

	common::UpdateSSState(recs);

	const bool retval = common::CheckForFailure(g_failedParams, 0, g_succeededParams);

	return std::make_pair(retval, recs);
}

himan::raw_time StringToTime(const std::string& dateTime, const std::string& mask)
{
	if (mask == "%Y-%m-%d %H:%M:%S" || mask == "%Y%m%d%H%M%S" || mask == "%Y%m%d%H%M")
	{
		return himan::raw_time(dateTime, mask);
	}

	const std::string s1(
	    "(seconds?|hours?|days?) since ([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})[\\sT]?([0-9]{2}):([0-9]{2}):([0-9]{2})Z?");
	const std::regex r1(s1);
	std::smatch what;

	if (std::regex_match(mask, what, r1))
	{
		if (what.size() != 8)
		{
			return himan::raw_time();
		}

		const auto timeUnit = what.str(1);

		const auto year = what.str(2);
		std::string month = what.str(3);
		std::string day = what.str(4);

		if (month.length() < 2)
		{
			month = "0" + month;
		}
		if (day.length() < 2)
		{
			day = "0" + day;
		}

		himan::raw_time base(year + month + day + what.str(5) + what.str(6) + what.str(7), "%Y%m%d%H%M%S");

		double scale = 1;  // hours

		if (timeUnit == "seconds")
		{
			scale = 1 / 3600.;
		}
		else if (timeUnit == "days")
		{
			scale = 24;
		}

		base.Adjust(himan::kHourResolution, static_cast<int>(scale * std::stod(dateTime)));

		return base;
	}

	return himan::raw_time();
}
