#include "netcdfloader.h"
#include "NFmiNetCDF.h"
#include "common.h"
#include "info.h"
#include "latitude_longitude_grid.h"
#include "options.h"
#include "plugin_factory.h"
#include <algorithm>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include <ctime>
#include <iomanip>
#include <ogr_spatialref.h>
#include <sstream>
#include <stdexcept>
#include <util.h>

#define HIMAN_AUXILIARY_INCLUDE
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;

using namespace grid_to_radon;

#define kFloatMissing 32700.f

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

	/*
	    if (first.Y() == static_cast<int>(kFloatMissing))
	    {
	        std::cerr << "Unable to determine first grid point coordinates" << std::endl;
	    }
	*/

	const double di = reader.XResolution();
	const double dj = reader.YResolution();

	if (reader.Projection() == "latitude_longitude")
	{
		return std::unique_ptr<himan::latitude_longitude_grid>(new himan::latitude_longitude_grid(
		    himan::kBottomLeft, first, ni, nj, di, dj, himan::earth_shape<double>()));
	}
	/*
	    else if (reader.Projection() == "rotated_latitude_longitude")
	    {
	        info.projection = "rll";
	        info.gridtype = 10;
	    }

	    else if (reader.Projection() == "polar_stereographic")
	    {
	        info.projection = "polster";
	        info.gridtype = 5;
	    }

	    else if (reader.Projection() == "lambert_conformal_conic")
	    {
	        info.projection = "lcc";
	        info.gridtype = 3;
	    }
	*/
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

	// If parameter list is specified, check that parameter belongs to it

	/*
	    if (parameters.size() > 0)
	    {
	        if (std::find(parameters.begin(), parameters.end(), par.Name()) == parameters.end())
	        {
	            itsLogger.Warning("Skipping parameter " + par.Name());

	            g_failedParams++;
	            pskip[ncname] = 1;
	            continue;
	        }
	    }
	*/
}

bool NetCDFLoader::Load(const std::string& theInfile)
{
	NFmiNetCDF reader;

	if (!reader.Read(theInfile))
	{
		return false;
	}

	// reader.AnalysisTime(options.analysistime);

	if (options.analysistime.empty())
	{
		itsLogger.Error("Analysistime not specified");
		return false;
	}

	if (!reader.IsConvention())
	{
		itsLogger.Error("File '" + theInfile + "' is not CF conforming NetCDF");
		return false;
	}

	itsLogger.Debug("Read " + std::to_string(reader.SizeZ()) + " levels,\n" + +"     " +
	                std::to_string(reader.SizeX()) + " x coordinates,\n" + "     " + std::to_string(reader.SizeY()) +
	                " y coordinates,\n" + "     " + std::to_string(reader.SizeT()) + " timesteps,\n" + "     " +
	                std::to_string(reader.SizeParams()) + " parameters from file '" + theInfile + "'");

	/* Set struct fcinfo accordingly */

	if (options.producer == 0)
	{
		itsLogger.Error("producer_id value not found");
		return false;
	}

	const himan::producer prod(options.producer);

	if (options.analysistime.size() < 10)
	{
		itsLogger.Error("Invalid format for analysistime: " + options.analysistime);
		itsLogger.Error("Use YYYYMMDDHH24[MI]");
		return false;
	}

	const himan::raw_time originTime = ReadTime(options.analysistime);

	auto geom = ReadAreaAndGrid(reader);

	auto r = GET_PLUGIN(radon);

	auto geomdef = r->RadonDB().GetGeometryDefinition(geom->Ni(), geom->Nj(), geom->FirstPoint().Y(),
	                                                  geom->FirstPoint().X(), geom->Di(), geom->Dj(), geom->Type());

	if (geomdef.empty())
	{
		itsLogger.Error("Did not find geometry from database");
		return false;
	}

	auto config = std::make_shared<himan::configuration>();

	config->WriteToDatabase(true);
	config->WriteMode(himan::kSingleGridToAFile);
	config->DatabaseType(himan::kRadon);
	config->TargetGeomName(geomdef["name"]);

	auto CreateInfo = [&geom, &prod](const himan::forecast_type& ftype, const himan::forecast_time& ftime,
	                                 const himan::level& lvl,
	                                 const himan::param& par) -> std::shared_ptr<himan::info<double>> {
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

	auto Write = [&](std::shared_ptr<himan::info<double>>& info) -> bool {
		const std::string theFileName = common::MakeFileName(config, info, "");

		himan::file_information finfo;
		finfo.file_type = himan::kNetCDF;
		finfo.file_location = theFileName;
		finfo.file_server = itsHostName;
		finfo.message_no = boost::none;
		finfo.offset = boost::none;
		finfo.length = boost::none;
		finfo.storage_type = himan::kLocalFileSystem;

		if (!options.dry_run)
		{
			if (!reader.WriteSlice(finfo.file_location))
			{
				itsLogger.Error("Write to file failed");
				return false;
			}

			if (!grid_to_radon::common::SaveToDatabase(config, info, r, finfo, ssStateInformation))
			{
				itsLogger.Error("Write to radon failed");
				return false;
			}
			itsLogger.Debug("Wrote " + info->Param().Name() + " level " + static_cast<std::string>(info->Level()) +
			                " to file '" + finfo.file_location + "'");
		}

		return true;
	};

	const himan::forecast_type ftype(himan::kDeterministic);

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
				auto levname = boost::to_upper_copy(options.level);
				lvl = himan::level(himan::HPStringToLevelType.at(levname), 0);
			}

			if (!reader.HasDimension("z"))
			{
				// This parameter has no z dimension --> map to level 0

				auto info =
				    CreateInfo(ftype, ftime, lvl, himan::util::GetParameterInfoFromDatabaseName(prod, par, lvl));
				Write(info);
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

					auto info =
					    CreateInfo(ftype, ftime, lvl, himan::util::GetParameterInfoFromDatabaseName(prod, par, lvl));

					Write(info);
				}
			}
			g_succeededParams++;
		} while (reader.NextParam());
	}
	itsLogger.Info("Success with " + std::to_string(g_succeededParams) + " params, failed with " +
	               std::to_string(g_failedParams) + " params");

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

himan::raw_time StringToTime(const std::string& dateTime, const std::string& mask)
{
	if (mask == "%Y-%m-%d %H:%M:%S" || mask == "%Y%m%d%H%M%S" || mask == "%Y%m%d%H%M")
	{
		return himan::raw_time(dateTime, mask);
	}

	const std::string s1("(seconds|hours) since ([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})");
	const boost::regex r1(s1);
	boost::smatch what;

	if (boost::regex_match(mask, what, r1))
	{
		if (what.size() != 8)
		{
			//			cerr << "Unable to match mask " << mask << " with regex\n";
			//			exit(1);
			return himan::raw_time();
		}

		const auto timeUnit = what[1];

		himan::raw_time base(what[2] + what[3] + what[4] + what[5] + what[6] + what[7], "%Y%m%d%H%M%S");

		double scale = 1;

		if (timeUnit == "seconds")
		{
			scale = 1 / 3600.;
		}

		base.Adjust(himan::kHourResolution, static_cast<int>(scale * std::stod(dateTime)));

		return base;
	}

	return himan::raw_time();
}