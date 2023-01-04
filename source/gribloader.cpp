#include "gribloader.h"
#include "common.h"
#include "plugin_factory.h"
#include "timer.h"
#include "util.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <iomanip>
#include <sstream>
#include <stdlib.h>

#define HIMAN_AUXILIARY_INCLUDE
#include "grib.h"
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;

using namespace std;

bool grib1CacheInitialized = false, grib2CacheInitialized = false;
std::mutex recordUpdateMutex;

grid_to_radon::GribLoader::GribLoader() : g_success(0), g_skipped(0), g_failed(0)
{
}

set<string> CheckForMultiTableGribs(const grid_to_radon::records& recs)
{
	set<string> tables;
	for_each(recs.begin(), recs.end(), [&](const grid_to_radon::record& rec) { tables.insert(rec.table_name); });

	return tables;
}

pair<bool, grid_to_radon::records> grid_to_radon::GribLoader::Load(const string& theInfile)
{
	itsInputFileName = theInfile;
	itsReader.Open(theInfile);

	vector<boost::thread> threadgroup;

	for (short i = 0; i < options.threadcount; i++)
	{
		threadgroup.push_back(boost::thread(&GribLoader::Run, this, i));
	}

	for (auto& t : threadgroup)
	{
		t.join();
	}

	himan::logger logr("gribloader");
	logr.Info(fmt::format("Success with {} fields, failed with {} fields, skipped {} fields", g_success, g_failed,
	                      g_skipped));

	if (options.in_place_insert)
	{
		const auto tables = CheckForMultiTableGribs(itsRecords);

		if (options.allow_multi_table_gribs == false && tables.size() > 1)
		{
			throw std::runtime_error(
			    fmt::format("Gribs in file '{}' are loaded to multiple tables: {}", theInfile, fmt::join(tables, ",")));
		}
	}

	const bool retval = common::CheckForFailure(g_skipped, g_failed, g_success);

	if (retval)
	{
		grid_to_radon::common::UpdateSSState(itsRecords);
	}

	return make_pair(retval, itsRecords);
}

void grid_to_radon::GribLoader::Run(short threadId)
{
	himan::logger logr("gribloader#" + to_string(threadId));
	logr.Info("Started");

	NFmiGribMessage myMessage;
	unsigned int messageNo;

	while (DistributeMessages(myMessage, messageNo))
	{
		Process(myMessage, threadId, messageNo);
	}

	logr.Info("Stopped");
}

bool grid_to_radon::GribLoader::DistributeMessages(NFmiGribMessage& newMessage, unsigned int& messageNo)
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

std::pair<std::shared_ptr<himan::configuration>, std::shared_ptr<himan::info<double>>> ReadMetadata(
    const NFmiGribMessage& message)
{
	auto gribpl = GET_PLUGIN(grib);

	auto config = std::make_shared<himan::configuration>();

	config->WriteToDatabase(true);
	config->WriteMode(himan::kSingleGridToAFile);
	config->DatabaseType(himan::kRadon);
	config->OutputFileType(static_cast<himan::HPFileType>(message.Edition()));
	config->ProgramName(himan::kGridToRadon);

	auto info = std::make_shared<himan::info<double>>();

	himan::plugin::search_options opts(himan::forecast_time(), himan::param(), himan::level(), himan::producer(),
	                                   std::make_shared<himan::plugin_configuration>(*config));

	if (gribpl->CreateInfoFromGrib<double>(opts, false, true, info, message, false) == false ||
	    info->Producer().Id() == himan::kHPMissingInt)
	{
		throw himan::kFileMetaDataNotFound;
	}

	auto radonpl = GET_PLUGIN(radon);
	const auto geom = std::dynamic_pointer_cast<himan::regular_grid>(info->Grid());
	if (!geom)
	{
		himan::Abort();
	}

	// FirstPoint always returns "normal" latitude and longitude, but in radon we have
	// rotated coordinates as the first point for rotated_latitude_longitude

	const himan::point fp =
	    (geom->Type() == himan::kRotatedLatitudeLongitude)
	        ? dynamic_pointer_cast<himan::rotated_latitude_longitude_grid>(geom)->Rotate(geom->FirstPoint())
	        : geom->FirstPoint();

	auto geomdef = radonpl->RadonDB().GetGeometryDefinition(geom->Ni(), geom->Nj(), fp.Y(), fp.X(), geom->Di(),
	                                                        geom->Dj(), geom->Type());

	if (geomdef.empty())
	{
		himan::logger logr("gribloader");
		logr.Warning(fmt::format("Geometry not found from radon: type '{}' first point {},{} ni/nj {} {} di/dj {} {}",
		                         himan::HPGridTypeToString.at(geom->Type()), fp.X(), fp.Y(), geom->Ni(), geom->Nj(),
		                         geom->Di(), geom->Dj()));

		throw himan::kFileMetaDataNotFound;
	}

	config->TargetGeomName(geomdef["name"]);

	return make_pair(config, info);
}

void WriteMessage(NFmiGribMessage& message, const std::string& theFileName)
{
	if (!options.dry_run && !options.in_place_insert)
	{
		grid_to_radon::common::CreateDirectory(theFileName);

		if (!message.Write(theFileName, false))
		{
			throw std::runtime_error("Message write failed");
		}
	}
}

void grid_to_radon::GribLoader::Process(NFmiGribMessage& message, short threadId, unsigned int messageNo)
{
	himan::timer msgtimer(true);
	himan::logger logr("gribloader#" + to_string(threadId));

	try
	{
		auto metadata = ReadMetadata(message);

		auto config = metadata.first;
		auto info = metadata.second;

		const string theFileName = grid_to_radon::common::MakeFileName(config, info, itsInputFileName);

		himan::timer tmr(true);

		WriteMessage(message, theFileName);

		tmr.Stop();
		const size_t writeTime = tmr.GetTime();

		tmr.Start();

		auto r = GET_PLUGIN(radon);

		himan::file_information finfo;
		finfo.storage_type = himan::kLocalFileSystem;
		finfo.message_no = (options.in_place_insert) ? messageNo : 0;
		finfo.offset = (options.in_place_insert) ? itsReader.Offset(messageNo) : 0UL;
		finfo.length = static_cast<unsigned long>(message.GetLongKey("totalLength"));
		finfo.file_location = theFileName;
		finfo.file_type = static_cast<himan::HPFileType>(message.Edition());

		if (info->Param().Name() != "XX-X")
		{
			auto ret = grid_to_radon::common::SaveToDatabase(config, info, r, finfo);

			if (ret.first)
			{
				tmr.Stop();
				const size_t databaseTime = tmr.GetTime();

				g_success++;
				msgtimer.Stop();

				const size_t messageTime = msgtimer.GetTime();
				const size_t otherTime = messageTime - writeTime - databaseTime;

				string logmsg = fmt::format(
				    "Message {} producer {} analysis time {} step {} parameter {} level {} forecast type {} write "
				    "time={} "
				    "database time={} other={} "
				    "total={} ms",
				    messageNo, info->Producer().Id(), static_cast<string>(info->Time().OriginDateTime()),
				    static_cast<string>(info->Time().Step()), info->Param().Name(), static_cast<string>(info->Level()),
				    static_cast<string>(info->ForecastType()), writeTime, databaseTime, otherTime, messageTime);

				logr.Debug(logmsg);

				{
					std::lock_guard<std::mutex> lock(recordUpdateMutex);
					itsRecords.push_back(ret.second);
				}
			}
			else
			{
				g_failed++;
			}
		}
		else
		{
			g_failed++;
		}
	}
	catch (const himan::HPExceptionType& e)
	{
		g_failed++;

		if (e != himan::kFileMetaDataNotFound)
		{
			himan::Abort();
		}
	}
	catch (const std::exception& e)
	{
		logr.Error(e.what());
		g_failed++;
	}
	catch (...)
	{
		g_failed++;
	}
}
