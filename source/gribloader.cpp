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

grid_to_radon::GribLoader::GribLoader() : g_success(0), g_skipped(0), g_failed(0)
{
}

bool grid_to_radon::GribLoader::Load(const string& theInfile)
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
	logr.Info("Success with " + to_string(g_success) + " fields, " + "failed with " + to_string(g_failed) +
	          " fields, " + "skipped " + to_string(g_skipped) + " fields");

	grid_to_radon::common::UpdateSSState(ssStateInformation);

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

	auto info = std::make_shared<himan::info<double>>();

	himan::plugin::search_options opts(himan::forecast_time(), himan::param(), himan::level(), himan::producer(),
	                                   std::make_shared<himan::plugin_configuration>(*config));

	if (gribpl->CreateInfoFromGrib<double>(opts, false, true, info, message, false) == false)
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
		auto ret = ReadMetadata(message);

		auto config = ret.first;
		auto info = ret.second;

		const string theFileName = grid_to_radon::common::MakeFileName(config, info, itsInputFileName);

		himan::timer tmr(true);

		WriteMessage(message, theFileName);

		tmr.Stop();
		const size_t writeTime = tmr.GetTime();

		tmr.Start();

		auto r = GET_PLUGIN(radon);

		himan::file_information finfo;
		finfo.storage_type = himan::kLocalFileSystem;
		finfo.message_no = messageNo;
		finfo.offset = (options.in_place_insert) ? itsReader.Offset(messageNo) : 0;
		finfo.length = static_cast<unsigned long>(message.GetLongKey("totalLength"));
		finfo.file_location = theFileName;
		finfo.file_type = static_cast<himan::HPFileType>(message.Edition());

		if (info->Param().Name() != "XX-X" &&
		    grid_to_radon::common::SaveToDatabase(config, info, r, finfo, ssStateInformation))
		{
			tmr.Stop();
			const size_t databaseTime = tmr.GetTime();

			g_success++;
			msgtimer.Stop();

			const size_t messageTime = msgtimer.GetTime();
			const size_t otherTime = messageTime - writeTime - databaseTime;

			logr.Debug("Message " + to_string(messageNo) + " parameter " + info->Param().Name() + " at level " +
			           static_cast<string>(info->Level()) + " forecast type " +
			           static_cast<string>(info->ForecastType()) + " write time=" + to_string(writeTime) +
			           ", database time=" + to_string(databaseTime) + ", other=" + to_string(otherTime) +
			           " total=" + to_string(messageTime) + " ms");
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
	catch (const std::invalid_argument& e)
	{
		logr.Error(e.what());
		g_failed++;
	}
	catch (const std::exception& e)
	{
		logr.Error(e.what());
		himan::Abort();
	}
	catch (...)
	{
		g_failed++;
	}
}
