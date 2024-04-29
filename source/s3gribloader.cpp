#include "s3gribloader.h"
#include "NFmiGrib.h"
#include "common.h"
#include "options.h"
#include "plugin_factory.h"
#include "s3.h"
#include "timer.h"
#include "util.h"
#include <iostream>
#include <stdexcept>
#include <string.h>
#include <string>
#include <vector>

#define HIMAN_AUXILIARY_INCLUDE
#include "grib.h"
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

extern grid_to_radon::Options options;
extern std::pair<std::shared_ptr<himan::configuration>, std::shared_ptr<himan::info<double>>> ReadMetadata(
    const NFmiGribMessage& message);

static int g_success = 0;
static int g_failed = 0;

grid_to_radon::records ProcessGribFile(std::unique_ptr<FILE> fp, const std::string& filename)
{
	himan::timer othertimer(true);
	himan::logger logr("s3gribloader");

	grid_to_radon::records recs;

	NFmiGrib reader;
	if (!reader.Open(std::move(fp)))
	{
		logr.Error("Failed to open file from memory");
		return recs;
	}

	int messageNo = -1;
	auto r = GET_PLUGIN(radon);

	const auto plainFilename = grid_to_radon::common::StripProtocol(filename);
	while (reader.NextMessage())
	{
		messageNo++;

		try
		{
			othertimer.Start();

			auto metadata = ReadMetadata(reader.Message());

			othertimer.Stop();

			auto config = metadata.first;
			auto info = metadata.second;

			himan::timer dbtimer(true);

			himan::file_information finfo;
			finfo.storage_type = himan::kS3ObjectStorageSystem;
			finfo.message_no = messageNo;
			finfo.offset = reader.Offset(messageNo);
			finfo.length = static_cast<unsigned long>(reader.Message().GetLongKey("totalLength"));
			finfo.file_location = plainFilename;
			finfo.file_type = static_cast<himan::HPFileType>(reader.Message().Edition());

			auto ret = grid_to_radon::common::SaveToDatabase(config, info, r, finfo);

			dbtimer.Stop();

			if (ret.first)
			{
				logr.Debug(fmt::format(
				    "Message {} producer {} analysistime {} step {} parameter {} level {} forecasttype {} db"
				    "time={} other={} ms",
				    messageNo, info->Producer().Id(), static_cast<std::string>(info->Time().OriginDateTime()),
				    static_cast<std::string>(info->Time().Step()), info->Param().Name(),
				    static_cast<std::string>(info->Level()), static_cast<std::string>(info->ForecastType()),
				    dbtimer.GetTime(), othertimer.GetTime()));

				g_success++;
				recs.push_back(ret.second);
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
	return recs;
}

grid_to_radon::S3GribLoader::S3GribLoader() : itsHost(nullptr)
{
	itsHost = getenv("S3_HOSTNAME");

	if (!itsHost)
	{
		throw std::runtime_error("Environment variable S3_HOSTNAME not defined");
	}
}

std::pair<bool, grid_to_radon::records> grid_to_radon::S3GribLoader::Load(const std::string& theFileName) const
{
	g_success = 0;
	g_failed = 0;
	grid_to_radon::records recs = ReadFileStream(theFileName, 0, 0);

	common::UpdateSSState(recs);

	himan::logger logr("s3gribloader");
	logr.Info(fmt::format("Success with {} fields, failed with {} fields", g_success, g_failed));

	bool retval = common::CheckForFailure(g_failed, 0, g_success);

	return std::make_pair(retval, recs);
}

grid_to_radon::records grid_to_radon::S3GribLoader::ReadFileStream(const std::string& theFileName, size_t startByte,
                                                                   size_t byteCount) const
{
	himan::logger logr("s3gribloader");

	himan::file_information finfo;
	finfo.message_no = std::nullopt;
	finfo.offset = startByte;
	finfo.length = byteCount;
	finfo.storage_type = himan::kS3ObjectStorageSystem;
	finfo.file_location = theFileName;
	finfo.file_server = itsHost;

	auto buffer = himan::s3::ReadFile(finfo);

	std::unique_ptr<FILE> fp(fmemopen(buffer.data, buffer.length, "r"));
	return ProcessGribFile(std::move(fp), theFileName);
}
