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

	cout << "Success with " << g_success << " fields, "
	     << "failed with " << g_failed << " fields, "
	     << "skipped " << g_skipped << " fields" << std::endl;

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
	printf("Thread %d started\n", threadId);

	NFmiGribMessage myMessage;
	unsigned int messageNo;

	while (DistributeMessages(myMessage, messageNo))
	{
		Process(myMessage, threadId, messageNo);
	}

	printf("Thread %d stopped\n", threadId);
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
		throw std::runtime_error("Failed to create info");
	}

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

	try
	{
		auto ret = ReadMetadata(message);

		auto config = ret.first;
		auto info = ret.second;

		const string theFileName = grid_to_radon::common::MakeFileName(config, info, itsInputFileName);

		himan::timer tmr(true);

		WriteMessage(message, theFileName);

		tmr.Stop();
		size_t writeTime = tmr.GetTime();

		tmr.Start();

		auto r = GET_PLUGIN(radon);

		himan::file_information finfo;
		finfo.storage_type = himan::kLocalFileSystem;
		finfo.message_no = messageNo;
		finfo.offset = (options.in_place_insert) ? itsReader.Offset(messageNo) : 0;
		finfo.length = static_cast<unsigned long>(message.GetLongKey("totalLength"));
		finfo.file_location = theFileName;
		finfo.file_type = static_cast<himan::HPFileType>(message.Edition());

		grid_to_radon::common::SaveToDatabase(config, info, r, finfo, ssStateInformation);

		tmr.Stop();
		size_t databaseTime = tmr.GetTime();

		g_success++;

		if (options.verbose)
		{
			const size_t messageTime = msgtimer.GetTime();
			const size_t otherTime = messageTime - writeTime - databaseTime;

			printf(
			    "Thread %d: Message %d parameter %s at level %s forecast type %s write time=%ld, database time=%ld, "
			    "other=%ld, "
			    "total=%ld ms\n",
			    threadId, messageNo, info->Param().Name().c_str(), static_cast<string>(info->Level()).c_str(),
			    static_cast<string>(info->ForecastType()).c_str(), writeTime, databaseTime, otherTime, messageTime);
		}
	}
	catch (...)
	{
		g_failed++;
	}
}
