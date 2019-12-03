#include "GribLoader.h"
#include "NFmiRadonDB.h"
#include "timer.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

extern Options options;

using namespace std;

mutex dirCreateMutex;

bool grib1CacheInitialized = false, grib2CacheInitialized = false;

namespace
{
bool GetGeometryInformation(BDAPLoader& databaseLoader, fc_info& info, short threadId)
{
	double di = info.di_degrees;
	double dj = info.dj_degrees;

	// METNO Analysis in NetCDF has d{i,j}_degrees.
	if (info.projection == "polster" || info.projection == "lcc")
	{
		di = info.di_meters;
		dj = info.dj_meters;
	}

	auto geominfo =
	    databaseLoader.RadonDB().GetGeometryDefinition(info.ni, info.nj, info.lat_degrees, info.lon_degrees, di, dj,
	                                                   static_cast<int>(info.ednum), static_cast<int>(info.gridtype));

	if (geominfo.empty())
	{
		printf(
		    "Thread %d: Geometry not found from radon, gridsize: %ldx%ld first point: %f,%f grid cell size: %fx%f "
		    "type: %ld\n",
		    threadId, info.ni, info.nj, info.lon_degrees, info.lat_degrees, di, dj, info.gridtype);
		return false;
	}

	info.geom_id = stol(geominfo["id"]);
	info.geom_name = geominfo["name"];

	return true;
}
}
void UpdateAsGrid(const std::set<std::string>& analyzeTables)
{
	BDAPLoader ldr;

	for (const auto& id : analyzeTables)
	{
		// update record_count column

		stringstream ss;

		// "table" contains table name with schema, ie schema.tablename.
		// We have to separate schema and table names; the combinition of
		// those is unique within a database so it's safe to update as_grid
		// based on just that information.

		std::vector<std::string> tokens;
		boost::split(tokens, id, boost::is_any_of(";"));

		std::vector<std::string> tableparts;
		boost::split(tableparts, tokens[0], boost::is_any_of("."));

		assert(tokens.size() == 2);

		ss << "UPDATE as_grid SET record_count = 1 WHERE schema_name = '" << tableparts[0] << "' AND partition_name = '"
		   << tableparts[1] << "' AND analysis_time = '" << tokens[1] << "'";

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
			ldr.RadonDB().Execute(ss.str());
		}
	}
}

void UpdateSSState(const std::set<std::string>& ssStateInformation)
{
	if (!options.ss_state_update)
	{
		return;
	}

	BDAPLoader ldr;

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

GribLoader::GribLoader() : g_success(0), g_skipped(0), g_failed(0)
{
	char myhost[128];
	gethostname(myhost, 128);
	itsHostName = string(myhost);
}
GribLoader::~GribLoader()
{
}

bool GribLoader::Load(const string& theInfile)
{
	inputFileName = theInfile;
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

	UpdateAsGrid(analyzeTables);
	UpdateSSState(ssStateInformation);

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

bool CopyMetaData(BDAPLoader& databaseLoader, fc_info& g, const NFmiGribMessage& message, short threadId)
{
	const auto centre = message.Centre();
	auto process = message.Process();

	g.length = static_cast<unsigned long>(message.GetLongKey("totalLength"));

	g.ednum = message.Edition();

	auto levtype = message.LevelType();

	if (g.ednum == 2)
	{
		const long secondLevelType = message.GetLongKey("typeOfSecondFixedSurface");

		// if two level types are defined and one is ground and other is
		// top of atmosphere, change level type to entire atmosphere
		// because radon does not support two leveltypes
		if (levtype == 1 && secondLevelType == 8)
		{
			levtype = 10;
		}
	}

	if (options.process != 0)
	{
		process = options.process;
	}

	// Find producer_id for this grid

	g.forecast_type_id = message.ForecastType();
	g.forecast_type_value =
	    (message.ForecastTypeValue() == -999) ? -1 : static_cast<double>(message.ForecastTypeValue());

	long producer_type_id = 1;  // deterministic forecast

	if (g.forecast_type_id == 2)
	{
		producer_type_id = 2;  // ANALYSIS
	}
	else if (g.forecast_type_id == 3 || g.forecast_type_id == 4)
	{
		producer_type_id = 3;  // ENSEMBLE
	}

	auto prodInfo = databaseLoader.RadonDB().GetProducerFromGrib(centre, process, producer_type_id);

	if (prodInfo.empty())
	{
		printf("Thread %d: Producer information not found from radon for centre %ld, process %ld, producer type %ld\n",
		       threadId, centre, process, producer_type_id);
		return false;
	}
	else if (prodInfo["class_id"] != "1")
	{
		printf("Thread %d: Producer class_id is %s, grid_to_neons can only handle gridded data (class_id = 1)\n",
		       threadId, prodInfo["class_id"].c_str());
		return false;
	}

	g.producer_id = stol(prodInfo["id"]);

	if (g.ednum == 1)
	{
		if (!grib1CacheInitialized)
		{
			databaseLoader.RadonDB().WarmGrib1ParameterCache(g.producer_id);
			grib1CacheInitialized = true;
		}

		auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(g.producer_id, levtype, g.ednum);

		if (levelinfo.empty())
		{
			printf("Thread %d: Level name not found for grib type %ld\n", threadId, levtype);
			return false;
		}

		g.levname = levelinfo["name"];
		g.levelid = stol(levelinfo["id"]);

		auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib1(
		    g.producer_id, message.Table2Version(), message.ParameterNumber(), message.TimeRangeIndicator(), levtype,
		    static_cast<double>(message.LevelValue()));

		if (paraminfo.empty())
		{
			if (options.verbose)
			{
				printf(
				    "Thread %d: Parameter name not found for table2Version %ld, number %ld, time range indicator %ld, "
				    "level type %ld\n",
				    threadId, message.Table2Version(), message.ParameterNumber(), message.TimeRangeIndicator(),
				    levtype);
			}

			return false;
		}

		g.paramid = stol(paraminfo["id"]);
		g.parname = paraminfo["name"];
	}
	else
	{
		if (!grib2CacheInitialized)
		{
			databaseLoader.RadonDB().WarmGrib2ParameterCache(g.producer_id);
			grib2CacheInitialized = true;
		}

		const long tosp = (message.TypeOfStatisticalProcessing() == -999) ? -1 : message.TypeOfStatisticalProcessing();

		auto paraminfo = databaseLoader.RadonDB().GetParameterFromGrib2(
		    g.producer_id, message.ParameterDiscipline(), message.ParameterCategory(), message.ParameterNumber(),
		    levtype, static_cast<double>(message.LevelValue()), tosp);

		if (paraminfo.empty())
		{
			if (options.verbose)
			{
				printf(
				    "Thread %d: parameter name not found for discipline %ld, category %ld, number %ld, statistical "
				    "processing %ld\n",
				    threadId, message.ParameterDiscipline(), message.ParameterCategory(), message.ParameterNumber(),
				    tosp);
			}

			return false;
		}

		g.parname = paraminfo["name"];
		g.paramid = stol(paraminfo["id"]);

		auto levelinfo = databaseLoader.RadonDB().GetLevelFromGrib(g.producer_id, levtype, g.ednum);

		if (!levelinfo.empty())
		{
			g.levname = levelinfo["name"];
			g.levelid = stol(levelinfo["id"]);
		}
	}

	if (g.levname.empty())
	{
		if (options.verbose)
		{
			printf("Thread %d: Level name not found for grib level %ld\n", threadId, levtype);
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

	g.lat_degrees = message.Y0();
	g.lon_degrees = message.X0();

	g.gridtype = message.GridType();
	switch (message.NormalizedGridType())
	{
		case 0:  // ll
			g.di_degrees = message.iDirectionIncrement();
			g.dj_degrees = message.jDirectionIncrement();
			g.di = g.di_degrees * 1000.;
			g.dj = g.dj_degrees * 1000.;
			g.projection = "ll";
			break;

		case 10:  // rll
			g.di_degrees = message.iDirectionIncrement();
			g.dj_degrees = message.jDirectionIncrement();
			g.di = g.di_degrees * 1000.;
			g.dj = g.dj_degrees * 1000.;
			g.projection = "rll";
			break;

		case 5:  // ps
			g.di = message.XLengthInMeters();
			g.dj = message.YLengthInMeters();
			g.di_meters = message.XLengthInMeters();
			g.dj_meters = message.YLengthInMeters();
			g.projection = "polster";
			break;

		case 3:  // lambert
			g.di_meters = message.XLengthInMeters();
			g.dj_meters = message.YLengthInMeters();
			g.projection = "lcc";
			break;

		case 4:  // reduced gg
			g.dj_degrees = message.jDirectionIncrement();
			g.projection = "rgg";
			break;

		default:
			printf("Thread %d: Unsupported gridType: %ld\n", threadId, message.NormalizedGridType());
			return false;
	}

	g.level1 = message.LevelValue();

	g.level2 = -1;  // "missing"

	if (levtype == 106 || levtype == 108 || levtype == 112)
	{
		g.level2 = message.LevelValue2();

		if (g.level2 == INT_MAX)
			g.level2 = -1;
	}

	g.timeUnit = message.UnitOfTimeRange();

	g.fcst_per = message.NormalizedStep(true, true);

	if (GetGeometryInformation(databaseLoader, g, threadId) == false)
	{
		return false;
	}

	return true;
}

void GribLoader::Run(short threadId)
{
	printf("Thread %d started\n", threadId);

	BDAPLoader dbLoader;

	NFmiGribMessage myMessage;
	unsigned int messageNo;

	while (DistributeMessages(myMessage, messageNo))
	{
		Process(dbLoader, myMessage, threadId, messageNo);
	}

	printf("Thread %d stopped\n", threadId);
}

bool GribLoader::DistributeMessages(NFmiGribMessage& newMessage, unsigned int& messageNo)
{
	lock_guard<mutex> lock(distMutex);

	if (itsReader.NextMessage())
	{
		messageNo = static_cast<unsigned int>(itsReader.CurrentMessageIndex());
		newMessage = NFmiGribMessage(itsReader.Message());
		return true;
	}

	return false;
}

void GribLoader::Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId, unsigned int messageNo)
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

	g.messageNo = (options.in_place_insert) ? messageNo : 0;
	g.offset = (options.in_place_insert) ? itsReader.Offset(messageNo) : 0;

	if (!CopyMetaData(databaseLoader, g, message, threadId))
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

	string theFileName;

	if (options.in_place_insert)
	{
		namespace fs = boost::filesystem;

		fs::path pathname(inputFileName);
		pathname = canonical(fs::system_complete(pathname));

		// Check that directory is in the form: /path/to/some/directory/<yyyymmddhh24mi>/<producer_id>/
		const auto atimedir = pathname.parent_path().filename();
		const auto proddir = pathname.parent_path().parent_path().filename();

		const boost::regex r1("\\d+");
		const boost::regex r2("\\d{12}");

		if (boost::regex_match(proddir.string(), r1) == false || boost::regex_match(atimedir.string(), r2) == false)
		{
			printf(
			    "Thread %d: File path must include analysistime and producer id "
			    "('/path/to/some/dir/<producer_id>/<analysistime12digits>/file.grib')\n",
			    threadId);
			printf("Thread %d: Got file '%s'\n", threadId, pathname.string().c_str());
			return;
		}
		string dirName = pathname.parent_path().string();

		// Input file name can contain path, or not.
		// Force full path to the file even if user has not given it

		theFileName = dirName + "/" + pathname.filename().string();
	}
	else
	{
		theFileName = GetFileName(databaseLoader, g);
		if (theFileName.empty())
		{
			return;
		}
	}

	g.filename = theFileName;
	g.filehost = itsHostName;

	timer tmr;
	tmr.start();

	if (!options.dry_run && !options.in_place_insert)
	{
		CreateDirectory(theFileName);
	}

	if (g.filename.empty())
	{
		exit(1);
	}

	CreateDirectory(g.filename);

	if (!options.dry_run && IsGrib(theFileName) && !options.in_place_insert)
	{
		if (!message.Write(g.filename, false))
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
		stringstream ss;
		ss << databaseLoader.LastInsertedTable() << ";" << g.year << "-" << setw(2) << setfill('0') << g.month << "-"
		   << setw(2) << setfill('0') << g.day << " " << setw(2) << setfill('0') << g.hour << ":" << setw(2)
		   << setfill('0') << g.minute << ":00";

		lock_guard<mutex> lock(tableMutex);

		analyzeTables.insert(ss.str());
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

		printf(
		    "Thread %d: Message %d parameter %s at level %s/%s %s write time=%ld, database time=%ld, other=%ld, "
		    "total=%ld ms\n",
		    threadId, g.messageNo.get(), g.parname.c_str(), g.levname.c_str(), lvl.c_str(), ftype.c_str(), writeTime,
		    databaseTime, otherTime, messageTime);
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
