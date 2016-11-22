#include "GribLoader.h"
#include "NFmiNeonsDB.h"
#include "NFmiRadonDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
#include <stdlib.h>

extern Options options;

using namespace std;

GribLoader::GribLoader() : g_success(0), g_skipped(0), g_failed(0) {}
GribLoader::~GribLoader() {}
bool GribLoader::Load(const string& theInfile)
{
	itsReader.Open(theInfile);

	// Read all message from file

	string levelString = options.leveltypes;

	if (!levelString.empty())
	{
		boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
	}

	string paramString = options.parameters;

	if (!paramString.empty())
	{
		boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
	}

	vector<boost::thread> threadgroup;

	for (short i = 0; i < options.threadcount; i++)
	{
		threadgroup.push_back(boost::thread(&GribLoader::Run, this, i));
	}

	for (unsigned short i = 0; i < threadgroup.size(); i++)
	{
		threadgroup[i].join();
	}

	cout << "Success with " << g_success << " fields, "
	     << "failed with " << g_failed << " fields, "
	     << "skipped " << g_skipped << " fields" << std::endl;

	// When dealing with options.max_failures and options.max_skipped, we regard -1 as "don't care" and all values >= 0
	// as something to be checked against.
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

/*
 * CopyMetaData()
 *
 * Read all necessary metadata from a grib message. Structure of function
 * copied from PutGribMsgToNeons_api() (putgribmsgtoneons_api.c:87)
 */

bool GribLoader::CopyMetaData(BDAPLoader& databaseLoader, fc_info& g, const NFmiGribMessage& message)
{
	g.centre = message.Centre();
	g.ednum = message.Edition();

	g.param = message.ParameterNumber();
	g.levtype = message.LevelType();

	g.process = message.Process();

	if (options.process != 0) g.process = options.process;

	// Default to deterministic forecast

	g.forecast_type_id = 1;

	g.forecast_type_id = message.ForecastType();
	g.forecast_type_value = (message.ForecastTypeValue() == -999) ? -1 : message.ForecastTypeValue();

	int producer_type = 1;  // deterministic

	if (g.forecast_type_id == 3 || g.forecast_type_id == 4)
	{
		producer_type = 3;  // ens
	}

	if (g.ednum == 1)
	{
		g.filetype = "grib";

		g.novers = message.Table2Version();
		g.timeRangeIndicator = message.TimeRangeIndicator();

		if (options.neons)
		{
			g.parname = databaseLoader.NeonsDB().GetGridParameterName(g.param, g.novers, g.novers, g.timeRangeIndicator,
			                                                          g.levtype);
			g.levname = databaseLoader.NeonsDB().GetGridLevelName(g.param, g.levtype, g.novers, g.novers);
		}
		else
		{
			auto prodinfo = databaseLoader.RadonDB().GetProducerFromGrib(g.centre, g.process, producer_type);

			if (prodinfo.empty())
			{
				if (options.verbose)
				{
					cerr << "FMI producer id not found for grib producer " << g.centre << " " << g.process << endl;
				}

				return false;
			}

			long producerId = boost::lexical_cast<long>(prodinfo["id"]);

			auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(producerId, g.levtype, g.ednum);

			if (levelinfo.empty())
			{
				cerr << "Level name not found for grib type " << g.levtype << endl;
				return false;
			}

			g.levname = levelinfo["name"];

			auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib1(
			    producerId, g.novers, g.param, g.timeRangeIndicator, g.levtype, message.LevelValue());
			g.parname = paraminfo["name"];
		}

		if (g.parname.empty())
		{
			if (options.verbose)
			{
				cerr << "Parameter name not found for table2Version " << g.novers << ", number " << g.param
				     << ", time range indicator " << g.timeRangeIndicator << " level type " << g.levtype << endl;
			}

			return false;
		}
	}
	else
	{
		g.filetype = "grib2";

		g.timeRangeIndicator = 0;

		if (options.neons)
		{
			g.parname = databaseLoader.NeonsDB().GetGridParameterNameForGrib2(g.param, message.ParameterCategory(),
			                                                                  message.ParameterDiscipline(), g.process);
			g.levname = databaseLoader.NeonsDB().GetGridLevelName(g.levtype, g.process);
		}
		else
		{
			auto prodinfo = databaseLoader.RadonDB().GetProducerFromGrib(g.centre, g.process, producer_type);

			if (prodinfo.empty())
			{
				if (options.verbose)
				{
					cerr << "FMI producer id not found for grib producer " << g.centre << " " << g.process << endl;
				}

				return false;
			}

			long producerId = boost::lexical_cast<long>(prodinfo["id"]);

			auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib2(producerId, message.ParameterDiscipline(),
			                                                                message.ParameterCategory(), g.param,
			                                                                g.levtype, message.LevelValue());

			if (!paraminfo.empty())
			{
				g.parname = paraminfo["name"];
			}

			auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(producerId, g.levtype, g.ednum);

			if (!levelinfo.empty())
			{
				g.levname = levelinfo["name"];
			}
		}

		g.category = message.ParameterCategory();
		g.discipline = message.ParameterDiscipline();

		if (g.parname.empty())
		{
			if (options.verbose)
			{
				cerr << "Parameter name not found for discipline " << message.ParameterDiscipline() << " category "
				     << message.ParameterCategory() << " number " << g.param << endl;
			}

			return false;
		}
	}

	if (g.levname.empty())
	{
		if (options.verbose)
		{
			cerr << "Level name not found for level " << g.levtype << endl;
		}
		return false;
	}

	g.year = message.Year();
	g.month = message.Month();
	g.day = message.Day();
	g.hour = message.Hour();
	g.minute = message.Minute();

	g.ni = message.SizeX();
	g.nj = message.SizeY();

	g.lat = message.Y0() * 1000;
	g.lon = message.X0() * 1000;

	g.lat_degrees = message.Y0();
	g.lon_degrees = message.X0();

	// This is because we need to find the
	// correct geometry from GRID_REG_GEOM in neons

	// GRIB2 longitudes --> GRIB1
	if (g.ednum == 2 && (g.lon > 180000))
	{
		g.lon -= 360000;
	}
	else if (g.ednum == 1 && g.lon == -180000 && g.centre != 86)
	{
		g.lon += 360000;  // Area is whole globe, ECMWF special case
	}

	g.gridtype = message.GridType();
	switch (message.NormalizedGridType())
	{
		case 0:  // ll
			g.di_degrees = message.iDirectionIncrement();
			g.dj_degrees = message.jDirectionIncrement();
			g.di = g.di_degrees * 1000.;
			g.dj = g.dj_degrees * 1000.;
			g.grtyp = "ll";
			break;

		case 10:  // rll
			g.di_degrees = message.iDirectionIncrement();
			g.dj_degrees = message.jDirectionIncrement();
			g.di = g.di_degrees * 1000.;
			g.dj = g.dj_degrees * 1000.;
			g.grtyp = "rll";
			break;

		case 5:  // ps
			g.di = message.XLengthInMeters();
			g.dj = message.YLengthInMeters();
			g.di_meters = message.XLengthInMeters();
			g.dj_meters = message.YLengthInMeters();
			g.grtyp = "polster";
			break;

		case 3:  // lambert
			g.di_meters = message.XLengthInMeters();
			g.dj_meters = message.YLengthInMeters();
			g.grtyp = "lambert";
			break;
		default:
			cerr << "Invalid geometry for GRIB: " << message.NormalizedGridType()
			     << ", only latlon, rotated latlon and polster are supported" << endl;
			return false;
			break;
	}

	stringstream ss;

	ss << g.year << setw(2) << setfill('0') << g.month << setw(2) << setfill('0') << g.day << setw(2) << setfill('0')
	   << g.hour << "0000";

	g.base_date = ss.str();

	g.level1 = message.LevelValue();
	g.lvl1_lvl2 = g.level1;

	g.level2 = -1;  // "missing"

	if (g.levtype == 106 || g.levtype == 112)
	{
		g.level2 = message.LevelValue2();

		if (g.level2 == INT_MAX) g.level2 = -1;
	}

	g.stepType = message.TimeRangeIndicator();
	g.timeUnit = message.UnitOfTimeRange();

	g.startstep = message.NormalizedStep(false, false);
	g.endstep = message.NormalizedStep(true, false);
	g.step = g.endstep;

	g.fcst_per = message.NormalizedStep(true, true);

	return true;
}

void GribLoader::Run(short threadId)
{
	printf("Thread %d started\n", threadId);

	itsThreadedLoader.reset(new BDAPLoader());

	NFmiGribMessage myMessage;

	while (DistributeMessages(myMessage))
	{
		Process(*itsThreadedLoader, myMessage, threadId);
	}

	printf("Thread %d stopped\n", threadId);
}

bool GribLoader::DistributeMessages(NFmiGribMessage& newMessage)
{
	lock_guard<mutex> lock(distMutex);
	if (itsReader.NextMessage())
	{
		if (options.verbose)
		{
			printf("Message %d\n", itsReader.CurrentMessageIndex());
		}

		newMessage = NFmiGribMessage(itsReader.Message());
		return true;
	}

	return false;
}

void GribLoader::Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId)
{
	fc_info g;

	timespec start_ms_ts, stop_ms_ts, start_ts, stop_ts;

	if (options.verbose)
	{
		clock_gettime(CLOCK_REALTIME, &start_ms_ts);
	}
	/*
	 * Read metadata from grib msg
	 */

	if (!CopyMetaData(databaseLoader, g, message))
	{
		g_skipped++;
		return;
	}

	if (parameters.size() > 0)
	{
		if (std::find(parameters.begin(), parameters.end(), g.parname) == parameters.end())
		{
			g_skipped++;
			return;
		}
	}

	if (levels.size() > 0)
	{
		if (std::find(levels.begin(), levels.end(), g.levname) == levels.end())
		{
			g_skipped++;
			return;
		}
	}

	string theFileName = GetFileName(databaseLoader, g);

	if (theFileName.empty()) exit(1);

	g.filename = theFileName;

	clock_gettime(CLOCK_REALTIME, &start_ts);

	CreateDirectory(theFileName);

	/*
	 * Write grib msg to disk with unique filename.
	 */

	if (!options.dry_run && IsGrib(theFileName))
	{
		if (!message.Write(theFileName, false))
		{
			g_failed++;
			return;
		}
	}

	clock_gettime(CLOCK_REALTIME, &stop_ts);
	size_t start = static_cast<size_t>(start_ts.tv_sec * 1000000000 + start_ts.tv_nsec);
	size_t stop = static_cast<size_t>(stop_ts.tv_sec * 1000000000 + stop_ts.tv_nsec);

	size_t writeTime = (stop - start) / 1000 / 1000;

	/*
	 * Update new file information to database
	 */

	clock_gettime(CLOCK_REALTIME, &start_ts);

	if (options.neons)
	{
		if (!databaseLoader.WriteAS(g))
		{
			g_failed++;
			return;
		}
	}

	if (options.radon)
	{
		if (!databaseLoader.WriteToRadon(g) && !options.neons)
		{
			g_failed++;
			return;
		}
	}

	clock_gettime(CLOCK_REALTIME, &stop_ts);
	start = static_cast<size_t>(start_ts.tv_sec * 1000000000 + start_ts.tv_nsec);
	stop = static_cast<size_t>(stop_ts.tv_sec * 1000000000 + stop_ts.tv_nsec);
	size_t databaseTime = (stop - start) / 1000 / 1000;

	g_success++;

	if (options.verbose)
	{
		clock_gettime(CLOCK_REALTIME, &stop_ms_ts);
		start = static_cast<size_t>(start_ms_ts.tv_sec * 1000000000 + start_ms_ts.tv_nsec);
		stop = static_cast<size_t>(stop_ms_ts.tv_sec * 1000000000 + stop_ms_ts.tv_nsec);
		size_t messageTime = (stop - start) / 1000 / 1000;

		size_t otherTime = messageTime - writeTime - databaseTime;

		string ftype = "";

		if (g.forecast_type_id > 2)
		{
			ftype = "forecast type " + boost::lexical_cast<string>(g.forecast_type_id) + "/" +
			        boost::lexical_cast<string>(g.forecast_type_value);
		}

		string lvl = boost::lexical_cast<string>(g.level1);
		if (g.level2 != -1)
		{
			lvl += "-" + boost::lexical_cast<string>(g.level2);
		}

		printf("Thread %d: Parameter %s at level %s/%s %s write time=%ld, database time=%ld, other=%ld, total=%ld ms\n",
		       threadId, g.parname.c_str(), g.levname.c_str(), lvl.c_str(), ftype.c_str(), writeTime, databaseTime,
		       otherTime, messageTime);
	}
}

string GribLoader::GetFileName(BDAPLoader& databaseLoader, const fc_info& g) { return databaseLoader.REFFileName(g); }
bool GribLoader::IsGrib(const string& theFileName)
{
	return theFileName.substr(theFileName.size() - 4, 4) == "grib" ||
	       theFileName.substr(theFileName.size() - 5, 5) == "grib2";
}

void GribLoader::CreateDirectory(const string& theFileName)
{
	namespace fs = boost::filesystem;

	fs::path pathname(theFileName);

	lock_guard<mutex> lock(dirCreateMutex);

	if (!fs::is_directory(pathname.parent_path()))
	{
		// Create directory

		if (options.verbose) cout << "Creating directory " << pathname.parent_path().string() << endl;

		if (!options.dry_run) fs::create_directories(pathname.parent_path());
	}
}
