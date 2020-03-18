#include "S3GribLoader.h"
#include "GribLoader.h"
#include "libs3.h"
#include "timer.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string.h>
#include <string>
#include <vector>

thread_local S3Status statusG = S3StatusOK;

static thread_local std::string _file_name;
static thread_local size_t _file_size;
// count bytes from the beginning of file (stream)
static thread_local long _file_bytes_from_start = 0;

thread_local std::set<std::string> analyzeTables, ssStateInformation;

extern Options options;
extern bool CopyMetaData(BDAPLoader& databaseLoader, fc_info& g, const NFmiGribMessage& message, short threadId);
extern void UpdateAsGrid(const std::set<std::string>& analyzeTables);
extern void UpdateSSState(const std::set<std::string>& ssStateInformation);
std::mutex tableMutex, ssMutex;

S3Status responsePropertiesCallback(const S3ResponseProperties* properties, void* callbackData)
{
	printf("File size is %ld bytes\n", properties->contentLength);
	FILE** fp = reinterpret_cast<FILE**>(callbackData);
	(*fp) = fmemopen(NULL, properties->contentLength, "rb+");

	if ((*fp) == NULL)
	{
		printf("fmemopen failed\n");
		exit(1);
	}

	return S3StatusOK;
}

static void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData)
{
	statusG = status;
	return;
}

bool ProcessGribMessage(std::unique_ptr<FILE> fp, const std::string& filename)
{
	auto StripProtocol = [](const std::string& fullFilename) {
		std::string ret = fullFilename;
		size_t start_pos = ret.find("s3://");

		if (start_pos != std::string::npos)
		{
			ret.erase(start_pos, 5);
		}

		return ret;
	};

	timer othertimer;
	othertimer.start();

	NFmiGrib reader;
	if (!reader.Open(std::move(fp)))
	{
		printf("Failed to open file from memory\n");
		return false;
	}

	BDAPLoader ldr;

	int messageNo = -1;
	while (reader.NextMessage())
	{
		messageNo++;
		const auto gribmsg = reader.Message();

		fc_info gribinfo;

		if (!CopyMetaData(ldr, gribinfo, gribmsg, 0))
		{
			continue;
		}

		gribinfo.messageNo = messageNo;
		gribinfo.offset = reader.Offset(messageNo);
		gribinfo.filename = StripProtocol(filename);
		gribinfo.filehost = std::string(getenv("S3_HOSTNAME"));
		gribinfo.fileprotocol = 2;

		othertimer.stop();
		timer dbtimer;
		dbtimer.start();

		ldr.WriteToRadon(gribinfo);

		if (ldr.NeedsAnalyze())
		{
			std::stringstream ss;

			using std::setfill;
			using std::setw;

			ss << ldr.LastInsertedTable() << ";" << gribinfo.year << "-" << setw(2) << setfill('0') << gribinfo.month
			   << "-" << setw(2) << setfill('0') << gribinfo.day << " " << setw(2) << setfill('0') << gribinfo.hour
			   << ":" << setw(2) << setfill('0') << gribinfo.minute << ":00";

			std::lock_guard<std::mutex> lock(tableMutex);

			analyzeTables.insert(ss.str());
		}

		{
<<<<<<< HEAD
			// message length from 'GRIB' to '7777'
			const long len = _file_bytes_from_start + workptr - _grib_message_offset + 1 - searchoffset;
			// printf("grib msg %d stopping (7777) at position %ld length %ld\n",_message_no, _grib_message_offset+len-1, len);

			// load message to database
			ProcessGribMessage(ret->buf, len, _grib_message_offset, _message_no);

			// copy the remaining bytes to a new memory location
			// and start searching for a new grib message
			const long newsize = ret->size - len;
			assert(ret->size >= len);
			char* newbuf = reinterpret_cast<char*>(malloc(newsize));
			memcpy(newbuf, &ret->buf[len], newsize);
=======
			std::lock_guard<std::mutex> lock(ssMutex);
			ssStateInformation.insert(ldr.LastSSStateInformation());
		}
>>>>>>> origin/development

		dbtimer.stop();

		if (options.verbose)
		{
			std::string ftype;

			if (gribinfo.forecast_type_id > 2)
			{
				ftype = "forecast type " + std::to_string(static_cast<int>(gribinfo.forecast_type_id)) + "/" +
				        std::to_string(static_cast<int>(gribinfo.forecast_type_value));
			}

			std::string lvl = std::to_string(static_cast<int>(gribinfo.level1));
			if (gribinfo.level2 != -1)
			{
				lvl += "-" + std::to_string(static_cast<int>(gribinfo.level2));
			}

			printf("Thread 0: Message %d parameter %s at level %s/%s %s database time=%d other=%d ms\n",
			       gribinfo.messageNo.get(), gribinfo.parname.c_str(), gribinfo.levname.c_str(), lvl.c_str(),
			       ftype.c_str(), dbtimer.get_time_ms(), othertimer.get_time_ms());
		}
	}
	return true;
}

static S3Status getObjectDataCallbackStreamProcessing(int bufferSize, const char* buffer, void* callbackData)
{
	FILE** fp = reinterpret_cast<FILE**>(callbackData);
	size_t wrote = fwrite(buffer, 1, bufferSize, *fp);
	fflush(*fp);
	return ((wrote < static_cast<size_t>(bufferSize)) ? S3StatusAbortedByCallback : S3StatusOK);
}

S3GribLoader::S3GribLoader() : itsHost(0), itsAccessKey(0), itsSecretKey(0)
{
	Initialize();
}

void S3GribLoader::Initialize()
{
	itsAccessKey = getenv("S3_ACCESS_KEY_ID");
	itsSecretKey = getenv("S3_SECRET_ACCESS_KEY");
	itsHost = getenv("S3_HOSTNAME");
	itsSecurityToken = getenv("S3_SESSION_TOKEN");

	if (!itsHost)
	{
		throw std::runtime_error("Environment variable S3_HOSTNAME not defined");
	}

	if (!itsAccessKey)
	{
		throw std::runtime_error("Environment variable S3_ACCESS_KEY_ID not defined");
	}

	if (!itsSecretKey)
	{
		throw std::runtime_error("Environment variable S3_SECRET_ACCESS_KEY not defined");
	}

	const auto ret = S3_initialize("s3", S3_INIT_ALL, itsHost);

	if (ret != S3StatusOK)
	{
		throw std::runtime_error("S3 initialization failed");
	}
}

bool S3GribLoader::Load(const std::string& theFileName) const
{
	ReadFileStream(theFileName, 0, 0);
	return true;
}

void S3GribLoader::ReadFileStream(const std::string& theFileName, size_t startByte, size_t byteCount) const
{
	// example: s3://noaa-gefs-pds/gefs.20170101/00/gep07.t00z.pgrb2bf036
	std::vector<std::string> tokens;
	boost::split(tokens, theFileName, boost::is_any_of("/"));

	const std::string bucket = tokens[2];

	tokens.erase(std::begin(tokens), std::begin(tokens) + 3);

	const std::string key = boost::algorithm::join(tokens, "/");

#ifdef S3_DEFAULT_REGION

	// extract region name from host name, assuming aws
	// s3.us-east-1.amazonaws.com
	boost::split(tokens, itsHost, boost::is_any_of("."));

	if (tokens.size() == 3)
	{
		std::cerr << "Hostname does not follow pattern s3.<regionname>.amazonaws.com" << std::endl;
		return;
	}

	const std::string region = tokens[1];

	// libs3 boilerplate

	// clang-format off

	S3BucketContext bucketContext =
	{
		itsHost,
		bucket.c_str(),
		S3ProtocolHTTP,
		S3UriStylePath,
		itsAccessKey,
		itsSecretKey,
		itsSecurityToken,
		region.c_str()
	};
#else
	S3BucketContext bucketContext =
	{
		itsHost,
		bucket.c_str(),
		S3ProtocolHTTP,
		S3UriStylePath,
		itsAccessKey,
		itsSecretKey,
		itsSecurityToken
	};
#endif

	// clang-format on

	std::cerr << "Input file: '" << theFileName << "' host: '" << itsHost << "' bucket: '" << bucket << "' key: '"
	          << key << "'" << std::endl;

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
		std::cout << "Stream-read file " << theFileName << " (" << S3_get_status_name(statusG) << ")" << std::endl;

		count++;

	} while (S3_status_is_retryable(statusG) && count < 3);

	rewind(fp);
	std::unique_ptr<FILE> ufp;
	ufp.reset(fp);

	ProcessGribMessage(std::move(ufp), theFileName);

	switch (statusG)
	{
		case S3StatusOK:
			UpdateAsGrid(analyzeTables);
			UpdateSSState(ssStateInformation);
			ssStateInformation.clear();
			analyzeTables.clear();
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
}
