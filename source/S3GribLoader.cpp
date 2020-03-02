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
thread_local std::set<std::string> analyzeTables, ssStateInformation;

extern Options options;
extern bool CopyMetaData(BDAPLoader& databaseLoader, fc_info& g, const NFmiGribMessage& message, short threadId);
extern void UpdateAsGrid(const std::set<std::string>& analyzeTables);
extern void UpdateSSState(const std::set<std::string>& ssStateInformation);
std::mutex tableMutex, ssMutex;

struct sink
{
	char* buf;
	long size;
	int first_call;

	sink() : buf(0), size(0), first_call(1)
	{
	}
	~sink()
	{
		if (buf)
		{
			free(buf);
		}
	}
};

S3Status responsePropertiesCallback(const S3ResponseProperties* properties, void* callbackData)
{
	printf("File size is %ld bytes\n", properties->contentLength);
	return S3StatusOK;
}

static void responseCompleteCallback(S3Status status, const S3ErrorDetails* error, void* callbackData)
{
	statusG = status;
	return;
}

bool SearchForGribStart(char* buf, int len, int& ptr)
{
	unsigned long stencil = 0;

	for (; ptr < len; ptr++)
	{
		stencil <<= 8;
		stencil |= buf[ptr];
		if ((stencil & 0xffffffff) == 0x47524942)
		{
			ptr -= 3;
			return true;
		}
	}

	return false;
}

bool SearchForGribStop(char* buf, int len, int& ptr)
{
	unsigned long stencil = 0;
	for (; ptr < len; ptr++)
	{
		stencil <<= 8;
		stencil |= buf[ptr];

		if ((stencil & 0xffffffff) == 0x37373737)
		{
			// We check for grib stop by finding 7777 AND checking that
			// the buffer ends OR that the another grib is following
			// (sometimes there are false 7777 sequences in the data).
			//
			// Obviously a better method would be to read the grib
			// size from the grib headers, but that is not easy to
			// implement without reusing gribapi components that are
			// not designed to be reused.
			//
			// Especially with large gribs (size > 8MB) the header
			// octets 5-7 do not tell the real length of the file; you have
			// read sector4 size and add some bytes from there with a
			// logic that is not self-evident.
			//
			// See "static int read_GRIB(reader* r)" at grib_io.c for more
			// proof.

			if (ptr + 4 < len &&
			    (buf[ptr + 1] != 'G' || buf[ptr + 2] != 'R' || buf[ptr + 3] != 'I' || buf[ptr + 4] != 'B'))
			{
				return false;
			}
			return true;
		}
	}

	return false;
}

bool ProcessGribMessage(char* ptr, long len, long offset, int messageNo)
{
	auto StripProtocol = [](const std::string& filename) {
		std::string ret = filename;
		size_t start_pos = filename.find("s3://");

		if (start_pos != std::string::npos)
		{
			ret.erase(start_pos, 5);
		}

		return ret;
	};

	timer othertimer;
	othertimer.start();

	NFmiGribMessage gribmsg(ptr, len);
	BDAPLoader ldr;

	fc_info gribinfo;

	if (!CopyMetaData(ldr, gribinfo, gribmsg, 0))
	{
		return false;
	}

	gribinfo.messageNo = messageNo;
	gribinfo.offset = offset;
	gribinfo.length = len;
	gribinfo.filename = StripProtocol(_file_name);
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

		ss << ldr.LastInsertedTable() << ";" << gribinfo.year << "-" << setw(2) << setfill('0') << gribinfo.month << "-"
		   << setw(2) << setfill('0') << gribinfo.day << " " << setw(2) << setfill('0') << gribinfo.hour << ":"
		   << setw(2) << setfill('0') << gribinfo.minute << ":00";

		std::lock_guard<std::mutex> lock(tableMutex);

		analyzeTables.insert(ss.str());
	}

	{
		std::lock_guard<std::mutex> lock(ssMutex);
		ssStateInformation.insert(ldr.LastSSStateInformation());
	}

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
		       gribinfo.messageNo.get(), gribinfo.parname.c_str(), gribinfo.levname.c_str(), lvl.c_str(), ftype.c_str(),
		       dbtimer.get_time_ms(), othertimer.get_time_ms());
	}

	return true;
}

static S3Status getObjectDataCallbackStreamProcessing(int bufferSize, const char* buffer, void* callbackData)
{
	// we accumulate read bytes to sink
	sink* ret = reinterpret_cast<sink*>(callbackData);

	// count bytes from the beginning of file (stream)
	static __thread long _file_bytes_from_start = 0;
	// offset of the current grib message from file start, -1 = message start not found
	static __thread long _grib_message_offset = -1;
	// message number of grib inside file
	static __thread int _message_no = 0;

	// If this is the first call of the callback function, initialize these variables
	// to zero (required if multiple files are loaded with a single grid_to_radon call)
	if (ret->first_call == 1)
	{
		ret->first_call = 0;
		_file_bytes_from_start = 0;
		_grib_message_offset = -1;
		_message_no = 0;
	}

	// reallocate more memory to fit the latest chunk returned from s3 library
	ret->buf = static_cast<char*>(realloc(ret->buf, ret->size + bufferSize));

	// copy newest chunk to the end of sink
	memcpy(ret->buf + ret->size, buffer, bufferSize);

	ret->size += bufferSize;

	// start reading from the beginning of the new chunk plus some
	// offset, because the grib message boundary could have been in between
	// two chunks
	char* work = ret->buf + (ret->size - bufferSize);
	int worklen = bufferSize;

	int searchoffset = 0;
	while (ret->size - bufferSize > 0 && searchoffset < 3)
	{
		work--;
		worklen++;
		searchoffset++;
	}

	// GRIB------------------7777GRIB-------------------7777GRIB----
	// |_________________________| = grib message
	// |______|______|_____|______|______|_____|____|____ = callback data chunks
	//                         |_________| = work pointer
	//                                 |_______| = work pointer
	//                         ^^^     ^^^ = overlap that is not counted to the grib message size

	while (true)
	{
		int workptr = 0;
		if (_grib_message_offset == -1)
		{
			if (SearchForGribStart(work, worklen, workptr) == false)
			{
				break;
			}

			// store the offset of the message start counting from file start
			// (that's what stored in the database)
			_grib_message_offset = workptr + _file_bytes_from_start - searchoffset;
			// printf("grib starting (GRIB) at position %ld\n", _grib_message_offset);
		}

		if (_grib_message_offset >= 0 && SearchForGribStop(work, worklen, workptr) == true)
		{
			// message length from 'GRIB' to '7777'
			const long len = _file_bytes_from_start + workptr - _grib_message_offset + 1 - searchoffset;
			// printf("grib stopping (7777) at position %ld\n", _grib_message_offset+len-1);

			// load message to database
			ProcessGribMessage(ret->buf, len, _grib_message_offset, _message_no);

			// copy the remaining bytes to a new memory location
			// and start searching for a new grib message
			const long newsize = ret->size - len;
			assert(ret->size >= len);
			char* newbuf = reinterpret_cast<char*>(malloc(newsize));
			memcpy(newbuf, &ret->buf[len], newsize);

			free(ret->buf);
			ret->buf = newbuf;
			ret->size = newsize;

			_message_no++;

			// restart search for the next grib message from the next byte
			// (right after the last '7'
			work = ret->buf;
			worklen -= workptr + 1 - searchoffset;

			// if there are more grib messages, the next one will be found immediately;
			// therefore increment the file byte counter and decrement the current buffer size
			_file_bytes_from_start += workptr + 1;
			bufferSize -= workptr + 1;

			_grib_message_offset = -1;
			continue;
		}

		break;
	}
	_file_bytes_from_start += bufferSize;
	return S3StatusOK;
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
		itsSecurityToken
	};

	std::cerr << "Input file: '" << theFileName << "' host: '" << itsHost << "' bucket: '" << bucket << "' key: '" << key << "'" << std::endl; 

	S3ResponseHandler responseHandler = 
	{
		&responsePropertiesCallback, 
		&responseCompleteCallback
	};

	// clang-format on

	S3GetObjectHandler getObjectHandler = {responseHandler, &getObjectDataCallbackStreamProcessing};

	int count = 0;
	do
	{
		sleep(count);

		sink buf;
		_file_name = theFileName;

		S3_get_object(&bucketContext, key.c_str(), NULL, startByte, byteCount, NULL, &getObjectHandler, &buf);
		std::cout << "Stream-read file " << theFileName << " (" << S3_get_status_name(statusG) << ")" << std::endl;

		count++;
	} while (S3_status_is_retryable(statusG) && count < 3);

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
