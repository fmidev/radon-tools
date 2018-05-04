#include "NetCDFLoader.h"
#include "NFmiNetCDF.h"
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "fc_info.h"
#include "options.h"
#include <algorithm>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

extern Options options;

using namespace std;

#define kFloatMissing 32700.f

static atomic<int> g_failedParams(0);
static atomic<int> g_succeededParams(0);

NetCDFLoader::NetCDFLoader()
{
	// Epoch() function handles UTC only

	setenv("TZ", "UTC", 1);
}

NetCDFLoader::~NetCDFLoader()
{
}
bool NetCDFLoader::Load(const string& theInfile)
{
	NFmiNetCDF reader;

	if (!reader.Read(theInfile))
	{
		return false;
	}

	// reader.AnalysisTime(options.analysistime);

	if (options.analysistime.empty())
	{
		cerr << "Analysistime not specified" << endl;
		return false;
	}

	if (!reader.IsConvention())
	{
		cerr << "File '" << theInfile << "' is not CF conforming NetCDF" << endl;
		return false;
	}

	if (options.verbose)
	{
		cout << "Read " << reader.SizeZ() << " levels," << endl
		     << "     " << reader.SizeX() << " x coordinates," << endl
		     << "     " << reader.SizeY() << " y coordinates," << endl
		     << "     " << reader.SizeT() << " timesteps " << endl
		     << "     " << reader.SizeParams() << " parameters from file '" << theInfile << "'" << endl;
	}

	/* Set struct fcinfo accordingly */

	fc_info info;

	info.centre = options.center;

	if (options.process == 0)
	{
		cerr << "process value not found" << endl;
		return false;
	}

	info.process = options.process;

	if (options.analysistime.size() < 10)
	{
		cerr << "Invalid format for analysistime: " << options.analysistime << endl;
		cerr << "Use YYYYMMDDHH24[MI]" << endl;
		return false;
	}

	info.year = boost::lexical_cast<int>(options.analysistime.substr(0, 4));
	info.month = boost::lexical_cast<int>(options.analysistime.substr(4, 2));
	info.day = boost::lexical_cast<int>(options.analysistime.substr(6, 2));
	info.hour = boost::lexical_cast<int>(options.analysistime.substr(8, 2));
	info.minute = 0;

	if (options.analysistime.length() > 10)
	{
		info.minute = boost::lexical_cast<int>(options.analysistime.substr(10, 2));
	}

	// ednum == 3 --> netcdf

	info.ednum = 3;
	info.level2 = 0;
	info.timeRangeIndicator = 0;
	info.timeUnit = 1;  // hour

	stringstream ss;

	ss << info.year << "-" << setw(2) << setfill('0') << info.month << "-" << setw(2) << setfill('0') << info.day << " "
	   << setw(2) << setfill('0') << info.hour << ":00:00";

	info.base_date = ss.str();

	info.ni = reader.SizeX();
	info.nj = reader.SizeY();

	if (reader.Projection() == "latitude_longitude")
	{
		info.grtyp = "ll";
		info.gridtype = 0;
	}

	else if (reader.Projection() == "rotated_latitude_longitude")
	{
		info.grtyp = "rll";
		info.gridtype = 10;
	}

	else if (reader.Projection() == "polar_stereographic")
	{
		info.grtyp = "polster";
		info.gridtype = 5;
	}

	else if (reader.Projection() == "lambert_conformal_conic")
	{
		info.grtyp = "lcc";
		info.gridtype = 3;
	}

	else
	{
		throw runtime_error("Unsupported projection: " + reader.Projection());
	}

	// If the file has both (x,y) and (lon,lat) variables, then choose (lon,lat) here.
	// (METNO Analysis)
	float lat = 0.0f;
	float lon = 0.0f;

	{
		NcError errorState (NcError::silent_nonfatal);

		if (reader.HasVariable("latitude") && reader.HasVariable("longitude"))
		{
			lat = reader.Lat0<float>();
			lon = reader.Lon0<float>();
		}
		else
		{
			lat = reader.Y0<float>();
			lon = reader.X0<float>();
		}
	}

	info.lat = static_cast<int>(1000.0f * lat);
	info.lon = static_cast<int>(1000.0f * lon);
	info.lat_degrees = lat;
	info.lon_degrees = lon;

	if (info.lat == static_cast<int>(kFloatMissing * 1000.))
	{
		std::cerr << "Unable to determine first grid point coordinates" << std::endl;
	}

	if (info.grtyp == "polster")
	{
		info.di_meters = reader.XResolution();
		info.dj_meters = reader.YResolution();
	}
	else
	{
		info.di_degrees = reader.XResolution();
		info.dj_degrees = reader.YResolution();
	}

	// we might consider to use round here, since floor can give unexpected results due to floating point precision
	info.di = floor(reader.XResolution() * 1000);
	info.dj = floor(reader.YResolution() * 1000);

	if (!itsDatabaseLoader.GetGeometryInformation(info))
	{
		return false;
	}

	long atimeEpoch = Epoch(options.analysistime, "%Y%m%d%H%M");

	map<string, short> pskip;

	vector<string> levels;

	string levelString = options.level;

	if (!levelString.empty())
	{
		boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
	}

	vector<string> parameters;

	string paramString = options.parameters;

	if (!paramString.empty())
	{
		boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
	}

	set<string> analyzeTables;

	for (reader.ResetTime(); reader.NextTime();)
	{
		long fctimeEpoch;

		switch (reader.TypeT())
		{
			case ncFloat:
				fctimeEpoch = Epoch(boost::lexical_cast<string>(reader.Time<float>()), reader.TimeUnit());
				break;

			case ncDouble:
				fctimeEpoch = Epoch(boost::lexical_cast<string>(reader.Time<double>()), reader.TimeUnit());
				break;

			case ncShort:
				fctimeEpoch = Epoch(boost::lexical_cast<string>(reader.Time<short>()), reader.TimeUnit());
				break;

			case ncInt:
				fctimeEpoch = Epoch(boost::lexical_cast<string>(reader.Time<int>()), reader.TimeUnit());
				break;
			case ncChar:
			case ncByte:
			case ncNoType:
			default:
				cout << "NcType not supported for time" << endl;
				exit(1);
		}

		float fctime = static_cast<float>(fctimeEpoch - atimeEpoch) / 3600.f;

		if (options.verbose)
		{
			cout << "Time " << static_cast<int>(reader.Time<float>()) << " (" << options.analysistime << " +" << fctime
			     << " hours)" << endl;
		}

		info.fcst_per = static_cast<int>(fctime);
		info.step = static_cast<int>(fctime);

		reader.FirstParam();

		do
		{
			string ncname = reader.Param()->name();

			if (ncname == "latitude" || ncname == "longitude" || ncname == "time" || ncname == "x" || ncname == "y")
				continue;

			info.ncname = ncname;

			// If this parameter is known not to be supported, skip it

			if (pskip.count(ncname) > 0)
			{
				continue;
			}

			string grid_parameter_name;
			map<string, string> parameter;

			parameter = itsDatabaseLoader.RadonDB().GetParameterFromNetCDF(info.process, ncname, -1, -1);
			grid_parameter_name = parameter["name"];

			if (grid_parameter_name.empty())
			{
				if (options.verbose)
					cout << "NetCDF param " << ncname << " not supported" << endl;

				g_failedParams++;
				pskip[ncname] = 1;
				continue;
			}

			// If parameter list is specified, check that parameter belongs to it

			if (parameters.size() > 0)
			{
				if (std::find(parameters.begin(), parameters.end(), grid_parameter_name) == parameters.end())
				{
					if (options.verbose)
						cout << "Skipping parameter " << grid_parameter_name << endl;

					g_failedParams++;
					pskip[ncname] = 1;
					continue;
				}
			}

			if (parameter.empty())
			{
				if (options.verbose)
					cout << "Param " << grid_parameter_name << " not supported" << endl;

				g_failedParams++;
				pskip[ncname] = 1;
				continue;
			}

			string name = parameter["parm_name"];

			info.parname = grid_parameter_name;

			if (options.verbose)
				cout << "Parameter " << ncname << " (" << grid_parameter_name << ")" << endl;

			float level = kFloatMissing;

			info.level2 = 0;

			// Check level type

			if (options.level.empty())
			{
				// Default

				info.levname = "HEIGHT";
				info.levtype = 105;
			}
			else
			{
				info.levname = boost::to_upper_copy(options.level);
				if (info.levname == "MEANSEA")
					info.levtype = 102;
				else if (info.levname == "DEPTH")
					info.levtype = 160;
				else if (info.levname == "HEIGHT")
					info.levtype = 105;
				else if (info.levname == "PRESSURE")
					info.levtype = 100;
				else
					throw std::runtime_error("Invalid level type: " + info.levname);
			}

			if (levels.size() > 0)
			{
				if (std::find(levels.begin(), levels.end(), info.levname) == levels.end())
				{
					if (options.verbose)
						cout << "Skipping level " << info.levname << endl;

					continue;
				}
			}

			if (!reader.HasDimension("z"))
			{
				// This parameter has no z dimension --> map to level 0

				level = 0;

				info.level1 = static_cast<int>(level);
				info.lvl1_lvl2 = info.level1 + 1000 * info.level2;

				string theFileName = itsDatabaseLoader.REFFileName(info);

				if (theFileName.empty())
					return false;

				info.filename = theFileName;

				if (!options.dry_run)
				{
					if (!reader.WriteSlice(theFileName))
					{
						return false;
					}
				}

				if (!itsDatabaseLoader.WriteToRadon(info))
				{
					return false;
				}

				if (itsDatabaseLoader.NeedsAnalyze())
				{
					const auto table = itsDatabaseLoader.LastInsertedTable();

					analyzeTables.insert(table);
				}

				if (options.verbose)
				{
					cout << "Wrote z-dimensionless data to file '" << theFileName << "'" << endl;
				}
			}
			else
			{
				for (reader.ResetLevel(); reader.NextLevel();)
				{
					if (options.use_level_value)
					{
						level = reader.Level();
					}
					else if (options.use_inverse_level_value)
					{
						level = reader.Level() * -1;
					}
					else
					{
						level = static_cast<float>(reader.LevelIndex());  // ordering number
					}
					info.level1 = static_cast<int>(level);
					info.lvl1_lvl2 = info.level1 + 1000 * info.level2;

					string theFileName = itsDatabaseLoader.REFFileName(info);

					if (theFileName.empty())
						return false;

					info.filename = theFileName;

					if (!options.dry_run)
					{
						if (!reader.WriteSlice(theFileName))
						{
							return false;
						}
					}

					if (!itsDatabaseLoader.WriteToRadon(info))
					{
						return false;
					}

					if (options.verbose)
					{
						cout << "Wrote level " << reader.LevelIndex() << " (" << level << ")"
						     << " to file '" << theFileName << "'" << endl;
					}
				}
			}
			g_succeededParams++;
		} while (reader.NextParam());
	}

	cout << "Success with " << g_succeededParams << " params, "
	     << "failed with " << g_failedParams << " params" << endl;

	for (const auto& table : analyzeTables)
	{
		if (options.verbose)
		{
			cout << "Analyzing table " << table << " due to first insert" << endl;
		}

		if (!options.dry_run)
		{
			itsDatabaseLoader.RadonDB().Execute("ANALYZE " + table);
		}

		std::vector<std::string> tokens;
		boost::split(tokens, table, boost::is_any_of("."));

		assert(tokens.size() == 2);

		ss.str("");
		ss << "UPDATE as_grid SET record_count = 1 WHERE schema_name = '" << tokens[0] << "' AND partition_name = '"
		   << tokens[1] << "'";

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

/*
 * Epoch()
 *
 * Convert a time to epoch. Date mask is mandatory.
 */

long NetCDFLoader::Epoch(const string& dateTime, const string& mask)
{
	struct tm t = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	char epoch[40];
	long e;

	boost::regex r1("seconds since ([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) UTC");

	boost::smatch sm;

	if (mask == "%Y-%m-%d %H:%M:%S" || mask == "%Y%m%d%H%M%S" || mask == "%Y%m%d%H%M")
	{
		strptime(dateTime.c_str(), mask.c_str(), &t);

		strftime(epoch, 40, "%s", &t);

		try
		{
			e = boost::lexical_cast<long>(epoch);
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "hours since 1900-01-01 00:00:00")
	{
		/*
		 * Negative epochs (epoch from times before 1970-01-01) are not guaranteed
		 * to work; use offset to adjust.
		 */

		long offset = 2208988800;  // seconds from 1900-01-01 to 1970-01-01

		try
		{
			e = (3600 * boost::lexical_cast<long>(dateTime)) - offset;
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "hours since 1970-01-01 00:00:00")
	{
		try
		{
			e = (3600 * boost::lexical_cast<long>(dateTime));
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "seconds since 1970-01-01 00:00:00 +00:00")
	{
		try
		{
			e = boost::lexical_cast<long>(dateTime);
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "seconds since 1970-01-01 00:00:00")
	{
		try
		{
			e = (boost::lexical_cast<long>(dateTime));
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "hours since 2014-01-01 00:00:00")
	{
		long offset = 1388534400;  // seconds from 1970-01-01 to 2014-01-01
		try
		{
			e = (3600 * boost::lexical_cast<long>(dateTime)) + offset;
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "hours since 1950-01-01 00:00:00")
	{
		long offset = 631152000;  // seconds from 1950-01-01 to 1970-01-01: "select extract(epoch from
		                          // '1970-01-01'::timestamp - '1950-01-01'::timestamp);"
		try
		{
			e = (3600 * boost::lexical_cast<long>(dateTime)) - offset;
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (mask == "hour since 1950-1-1T00:00:00Z")
	{
		long offset = 631152000;  // seconds from 1950-01-01 to 1970-01-01: "select extract(epoch from
		                          // '1970-01-01'::timestamp - '1950-01-01'::timestamp);"

		try
		{
			e = (3600 * boost::lexical_cast<long>(dateTime)) - offset;
		}
		catch (boost::bad_lexical_cast&)
		{
			cerr << "Date cast failed" << endl;
			exit(1);
		}
	}
	else if (boost::regex_match(mask, sm, r1))
	{
		tm stime;
		strptime(mask.c_str(), "seconds since %Y-%m-%d %H:%M:%S UTC", &stime);
		long offset = static_cast<long>(timegm(&stime));

		e = offset + boost::lexical_cast<long>(dateTime);
	}
	else
	{
		cerr << "Invalid time mask: '" << mask << "'" << endl;
		exit(1);
	}

	return e;
}
