#include "s3gribloader.h"
#include "NFmiGrib.h"
#include "common.h"
#include "libs3.h"
#include "options.h"
#include "plugin_factory.h"
#include "timer.h"
#include "util.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <stdexcept>
#include <string.h>
#include <string>
#include <vector>

#define HIMAN_AUXILIARY_INCLUDE
#include "grib.h"
#include "radon.h"
#undef HIMAN_AUXILIARY_INCLUDE

thread_local S3Status statusG = S3StatusOK;

extern grid_to_radon::Options options;
extern std::pair<std::shared_ptr<himan::configuration>, std::shared_ptr<himan::info<double>>> ReadMetadata(
    const NFmiGribMessage& message);

static int g_success = 0;
static int g_failed = 0;

S3Status responsePropertiesCallback(const S3ResponseProperties* properties, void* callbackData)
{
	himan::logger logr("s3gribloader");

	logr.Info(fmt::format("File size is {} bytes", properties->contentLength));
	FILE** fp = reinterpret_cast<FILE**>(callbackData);
	(*fp) = fmemopen(NULL, properties->contentLength, "rb+");

	if ((*fp) == NULL)
	{
		logr.Fatal("fmemopen failed");
		exit(1);
	}

	return S3StatusOK;
}

static void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData)
{
	statusG = status;
	return;
}

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
				    "Message {} step {} parameter {} level {} forecast type {} database time={} other={} ms", messageNo,
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

static S3Status getObjectDataCallbackStreamProcessing(int bufferSize, const char* buffer, void* callbackData)
{
	FILE** fp = reinterpret_cast<FILE**>(callbackData);
	size_t wrote = fwrite(buffer, 1, bufferSize, *fp);
	fflush(*fp);
	return ((wrote < static_cast<size_t>(bufferSize)) ? S3StatusAbortedByCallback : S3StatusOK);
}

grid_to_radon::S3GribLoader::S3GribLoader() : itsHost(0), itsAccessKey(0), itsSecretKey(0)
{
	itsAccessKey = getenv("S3_ACCESS_KEY_ID");
	itsSecretKey = getenv("S3_SECRET_ACCESS_KEY");
	itsHost = getenv("S3_HOSTNAME");
	itsSecurityToken = getenv("S3_SESSION_TOKEN");

	if (!itsHost)
	{
		throw std::runtime_error("Environment variable S3_HOSTNAME not defined");
	}

	himan::logger logr("s3gribloader");

	if (!itsAccessKey)
	{
		logr.Info("Environment variable S3_ACCESS_KEY_ID not defined");
	}

	if (!itsSecretKey)
	{
		logr.Info("Environment variable S3_SECRET_ACCESS_KEY not defined");
	}

	const auto ret = S3_initialize("s3", S3_INIT_ALL, itsHost);

	if (ret != S3StatusOK)
	{
		throw std::runtime_error("S3 initialization failed");
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
	// example: s3://noaa-gefs-pds/gefs.20170101/00/gep07.t00z.pgrb2bf036
	std::vector<std::string> tokens;
	boost::split(tokens, theFileName, boost::is_any_of("/"));

	const std::string bucket = tokens[2];

	tokens.erase(std::begin(tokens), std::begin(tokens) + 3);

	const std::string key = boost::algorithm::join(tokens, "/");

	S3Protocol protocol = S3ProtocolHTTP;

	try
	{
		const auto envproto = himan::util::GetEnv("S3_PROTOCOL");
		if (envproto == "https")
		{
			protocol = S3ProtocolHTTPS;
		}
		else if (envproto == "http")
		{
			protocol = S3ProtocolHTTP;
		}
		else
		{
			logr.Warning(fmt::format("Unrecognized value found from env variable S3_PROTOCOL: '{}'", envproto));
		}
	}
	catch (const std::invalid_argument& e)
	{
	}

	logr.Info(fmt::format("Using {} protocol to access data", protocol == S3ProtocolHTTP ? "http" : "https"));

#ifdef S3_DEFAULT_REGION

	const char* region = nullptr;

	if (std::string(itsHost).find("amazonaws") != std::string::npos)
	{
		// extract region name from host name, assuming
		// "s3.us-east-1.amazonaws.com"
		boost::split(tokens, itsHost, boost::is_any_of("."));

		if (tokens.size() == 4)
		{
			region = tokens[1].c_str();
		}
	}

	// libs3 boilerplate

	// clang-format off

	S3BucketContext bucketContext =
	{
		itsHost,
		bucket.c_str(),
		protocol,
		S3UriStylePath,
		itsAccessKey,
		itsSecretKey,
		itsSecurityToken,
		region
	};
#else
	S3BucketContext bucketContext =
	{
		itsHost,
		bucket.c_str(),
		protocol,
		S3UriStylePath,
		itsAccessKey,
		itsSecretKey,
		itsSecurityToken
	};
#endif

	// clang-format on

	logr.Info(fmt::format("Input file: '{}' host: '{}' bucket: '{}' key: '{}'", theFileName, itsHost, bucket, key));

	S3ResponseHandler responseHandler = {&responsePropertiesCallback, &responseCompleteCallback};
	S3GetObjectHandler getObjectHandler = {responseHandler, &getObjectDataCallbackStreamProcessing};

	int count = 0;
	FILE* fp = NULL;

	do
	{
		sleep(count);
#ifdef S3_DEFAULT_REGION
		S3_get_object(&bucketContext, key.c_str(), NULL, startByte, byteCount, NULL, 0, &getObjectHandler, &fp);
#else
		S3_get_object(&bucketContext, key.c_str(), NULL, startByte, byteCount, NULL, &getObjectHandler, &fp);
#endif
		logr.Debug(fmt::format("Stream-read file '{}' status: {}", theFileName, S3_get_status_name(statusG)));

		count++;

	} while (S3_status_is_retryable(statusG) && count < 3);

	if (fp == NULL)
	{
		logr.Error("Read failed");
		return {};
	}

	rewind(fp);
	std::unique_ptr<FILE> ufp;
	ufp.reset(fp);

	auto records = ProcessGribFile(std::move(ufp), theFileName);

	switch (statusG)
	{
		case S3StatusOK:
			break;
		case S3StatusInternalError:
			std::cerr << "ERROR S3 " << S3_get_status_name(statusG) << ": is there a proxy blocking the connection?"
			          << std::endl;
			break;
		case S3StatusFailedToConnect:
			std::cerr << "ERROR S3 " << S3_get_status_name(statusG) << ": is proxy required but not set?" << std::endl;
			break;
		case S3StatusErrorInvalidAccessKeyId:
			std::cerr << "ERROR S3 " << S3_get_status_name(statusG)
			          << ": are Temporary Security Credentials used without security token (env: S3_SESSION_TOKEN)?"
			          << std::endl;
			break;
		case S3StatusErrorPermanentRedirect:
			std::cerr << "ERROR S3 " << S3_get_status_name(statusG) << ": S3_HOSTNAME has wrong region?" << std::endl;
			break;

		default:
			std::cerr << "ERROR S3 " << S3_get_status_name(statusG) << std::endl;
	}

	return records;
}
