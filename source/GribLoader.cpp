#include "GribLoader.h"
#include "NFmiRadonDB.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

extern Options options;

using namespace std;

mutex dirCreateMutex;

struct timer
{
	timespec start_ts, stop_ts;

	void start()
	{
		clock_gettime(CLOCK_REALTIME, &start_ts);
	}
	void stop()
	{
		clock_gettime(CLOCK_REALTIME, &stop_ts);
	}
	int get_time_ms()
	{
		size_t _start = static_cast<size_t>(start_ts.tv_sec * 1000000000 + start_ts.tv_nsec);
		size_t _stop = static_cast<size_t>(stop_ts.tv_sec * 1000000000 + stop_ts.tv_nsec);
		return static_cast<int>(static_cast<double>(_stop - _start) / 1000. / 1000.);
	}
};

GribLoader::GribLoader() : g_success(0), g_skipped(0), g_failed(0)
{
}
GribLoader::~GribLoader()
{
}
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

	for (auto& t : threadgroup)
	{
		t.join();
	}

	cout << "Success with " << g_success << " fields, "
	     << "failed with " << g_failed << " fields, "
	     << "skipped " << g_skipped << " fields" << std::endl;

	BDAPLoader ldr;

	for (const auto& table : analyzeTables)
	{
		if (options.verbose)
		{
			cout << "Analyzing table " << table << " due to first insert" << endl;
		}

		if (!options.dry_run)
		{
			ldr.RadonDB().Execute("ANALYZE " + table);
		}
		else
		{
			cout << "ANALYZE " + table << endl;
		}
	}

	if (!options.ss_state_update)
	{
		for (const std::string& ssInfo : ssStateInformation)
		{
			vector<string> tokens;
			boost::split(tokens, ssInfo, boost::is_any_of("/"));

			stringstream ss;
			ss << "INSERT INTO ss_state (producer_id, geometry_id, analysis_time, forecast_period, forecast_type_id, "
			      "forecast_type_value, table_name) VALUES ("
			   << tokens[0] << ", " << tokens[1] << ", "
			   << "'" << tokens[2] << "', " << tokens[3] << ", " << tokens[4] << ", " << tokens[5] << ", "
			   << "'" << tokens[6] << "')";

			if (options.verbose)
			{
				cout << "Updating ss_state for " << ssInfo << endl;
			}

			if (options.dry_run)
			{
				cout << ss.str() << endl;
			}
			else
			{
				try
				{
					ldr.RadonDB().Execute(ss.str());
				}
				catch (const pqxx::unique_violation& e)
				{
					try
					{
						ss.str("");
						ss << "UPDATE ss_state SET last_updated = now() WHERE "
						   << "producer_id = " << tokens[0] << " AND "
						   << "geometry_id = " << tokens[1] << " AND "
						   << "analysis_time = '" << tokens[2] << "' AND "
						   << "forecast_period = " << tokens[3] << " AND "
						   << "forecast_type_id = " << tokens[4] << " AND "
						   << "forecast_type_value = " << tokens[5];

						ldr.RadonDB().Execute(ss.str());
					}
					catch (const pqxx::pqxx_exception& ee)
					{
						cerr << "Updating ss_state information failed" << endl;
					}
				}
			}
		}
	}

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

void CreateDirectory(const string& theFileName)
{
	namespace fs = boost::filesystem;

	fs::path pathname(theFileName);

	if (!fs::is_directory(pathname.parent_path()))
	{
		lock_guard<mutex> lock(dirCreateMutex);

		if (!fs::is_directory(pathname.parent_path()))
		{
			// Create directory

			if (options.verbose)
			{
				cout << "Creating directory " << pathname.parent_path().string() << endl;
			}

			if (!options.dry_run)
			{
				fs::create_directories(pathname.parent_path());
			}
		}
	}
}
/*
 * CopyMetaData()
 *
 * Read all necessary metadata from a grib message. Structure of function
 * copied from PutGribMsgToNeons_api() (putgribmsgtoneons_api.c:87)
 */

bool CopyMetaData(BDAPLoader& databaseLoader, fc_info& g, const NFmiGribMessage& message)
{
	g.ednum = message.Edition();

	g.param = message.ParameterNumber();
	g.levtype = message.LevelType();

	// If *centre* or *process* is specified on the command line, force them here.
	g.process = message.Process();
	if (options.process != 0)
		g.process = options.process;

	g.forecast_type_id = message.ForecastType();
	g.forecast_type_value =
	    (message.ForecastTypeValue() == -999) ? -1 : static_cast<double>(message.ForecastTypeValue());

	int producer_type = 1;  // deterministic

	if (g.forecast_type_id == 3 || g.forecast_type_id == 4)
	{
		producer_type = 3;  // ens
	}
	else if (g.forecast_type_id == 2)
	{
		producer_type = 2;  // analysis
	}

	auto prodinfo = databaseLoader.RadonDB().GetProducerFromGrib(g.centre, g.process, producer_type);

	if (prodinfo.empty())
	{
		if (options.verbose)
		{
			cerr << "FMI producer id not found for grib producer centre " << g.centre << " ident " << g.process
			     << " type " << producer_type << endl;
		}

		return false;
	}

	const long producerId = stol(prodinfo["id"]);

	if (g.ednum == 1)
	{
		g.novers = message.Table2Version();
		g.timeRangeIndicator = message.TimeRangeIndicator();

		databaseLoader.RadonDB().WarmGrib1ParameterCache(producerId);

		auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(producerId, g.levtype, g.ednum);

		if (levelinfo.empty())
		{
			cerr << "Level name not found for grib type " << g.levtype << endl;
			return false;
		}

		g.levname = levelinfo["name"];

		auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib1(
		    producerId, g.novers, g.param, g.timeRangeIndicator, g.levtype, static_cast<double>(message.LevelValue()));
		g.parname = paraminfo["name"];

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
		g.timeRangeIndicator = 0;

		databaseLoader.RadonDB().WarmGrib2ParameterCache(producerId);

		auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib2(producerId, message.ParameterDiscipline(),
		                                                                message.ParameterCategory(), g.param, g.levtype,
		                                                                static_cast<double>(message.LevelValue()));

		if (!paraminfo.empty())
		{
			g.parname = paraminfo["name"];
		}

		auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(producerId, g.levtype, g.ednum);

		if (!levelinfo.empty())
		{
			g.levname = levelinfo["name"];
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
			g.grtyp = "lcc";
			break;

		case 4:  // reduced gg
			g.dj_degrees = message.jDirectionIncrement();
			g.grtyp = "rgg";
			break;

		default:
			cerr << "Invalid geometry for GRIB: " << message.NormalizedGridType()
			     << ", only latlon, rotated latlon and polster are supported" << endl;
			return false;
			break;
	}

	stringstream ss;

	ss << g.year << "-" << setw(2) << setfill('0') << g.month << "-" << setw(2) << setfill('0') << g.day << " "
	   << setw(2) << setfill('0') << g.hour << ":" << g.minute << ":00";

	g.base_date = ss.str();

	g.level1 = message.LevelValue();
	g.lvl1_lvl2 = g.level1;

	g.level2 = -1;  // "missing"

	if (g.levtype == 106 || g.levtype == 108 || g.levtype == 112)
	{
		g.level2 = message.LevelValue2();

		if (g.level2 == INT_MAX)
			g.level2 = -1;
	}

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

	BDAPLoader dbLoader;

	NFmiGribMessage myMessage;

	while (DistributeMessages(myMessage))
	{
		Process(dbLoader, myMessage, threadId);
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

	timer msgtimer;

	if (options.verbose)
	{
		msgtimer.start();
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

	if (theFileName.empty())
		exit(1);

	g.filename = theFileName;

	timer tmr;
	tmr.start();

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

	tmr.stop();
	size_t writeTime = tmr.get_time_ms();

	/*
	 * Update new file information to database
	 */

	tmr.start();

	if (!databaseLoader.WriteToRadon(g))
	{
		g_failed++;
		return;
	}

	if (databaseLoader.NeedsAnalyze())
	{
		const auto table = databaseLoader.LastInsertedTable();

		lock_guard<mutex> lock(tableMutex);

		analyzeTables.insert(table);
	}

	{
		lock_guard<mutex> lock(ssMutex);
		ssStateInformation.insert(databaseLoader.LastSSStateInformation());
	}

	tmr.stop();
	size_t databaseTime = tmr.get_time_ms();

	g_success++;

	if (options.verbose)
	{
		msgtimer.stop();
		size_t messageTime = msgtimer.get_time_ms();

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

string GribLoader::GetFileName(BDAPLoader& databaseLoader, const fc_info& g)
{
	return databaseLoader.REFFileName(g);
}
bool GribLoader::IsGrib(const string& theFileName)
{
	return theFileName.substr(theFileName.size() - 4, 4) == "grib" ||
	       theFileName.substr(theFileName.size() - 5, 5) == "grib2";
}
