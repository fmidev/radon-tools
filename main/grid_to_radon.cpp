#include "geotiffloader.h"
#include "gribloader.h"
#include "netcdfloader.h"
#include "options.h"
#include "s3.h"
#include "s3gribloader.h"
#include "unistd.h"
#include <boost/program_options.hpp>
#include <chrono>
#include <filesystem>
#include <fmt/chrono.h>
#include <fstream>
#include <iostream>
#include <logger.h>
#include <thread>
#include <util.h>
grid_to_radon::Options options;
static himan::logger logr("grid_to_radon");

bool parse_options(int argc, char* argv[])
{
	namespace po = boost::program_options;

	po::options_description desc("Allowed options");

	bool no_ss_state_switch = false;
	bool no_directory_structure_check_switch = false;
	bool verbose = false;

	int max_failures = -1;
	int max_skipped = -1;

	int logLevel = -1;

	// clang-format off
	desc.add_options()
		("help,h", "print out help message")
		("verbose,v", po::bool_switch(&verbose), "set verbose mode on, equals to debug level 4")
                ("debug-level,d", po::value(&logLevel), "set log level: 0(fatal) 1(error) 2(warning) 3(info) 4(debug) 5(trace)")
		("netcdf,n", po::bool_switch(&options.netcdf), "force netcdf mode on")
		("grib,g", po::bool_switch(&options.grib), "force grib mode on")
		("geotiff,G", po::bool_switch(&options.geotiff), "force geotiff mode on")
		("version,V", "display version number")
		("infile,i", po::value<std::vector<std::string>>(&options.infile), "input file(s), - for stdin")
		("producer,p", po::value(&options.producer), "producer id")
		("analysistime,a", po::value(&options.analysistime), "force analysis time")
		("level,L", po::value(&options.level), "force level (only nc,geotiff)")
		("use-level-value", po::bool_switch(&options.use_level_value), "use level value instead of index")
		("use-inverse-level-value", po::bool_switch(&options.use_inverse_level_value), "use inverse level value instead of index")
		("max-failures", po::value(&max_failures), "maximum number of allowed loading failures (grib) -1 = \"don't care\"")
		("max-skipped", po::value(&max_skipped), "maximum number of allowed skipped messages (grib) -1 = \"don't care\"")
		("dry-run", po::bool_switch(&options.dry_run), "dry run (no changes made to database or disk), show all sql statements")
		("threads,j", po::value(&options.threadcount), "number of threads to use. only applicable to grib")
		("no-ss_state-update,X", po::bool_switch(&no_ss_state_switch), "do not update ss_state table information")
	        ("in-place,I", po::bool_switch(&options.in_place_insert), "do in-place insert (file not split and copied)")
	        ("no-directory-structure-check", po::bool_switch(&no_directory_structure_check_switch), "do not check for correct directory structure (in-place insert)")
		("smartmet-server-table-name", po::value(&options.ss_table_name), "override table name for smartmet server")
		("allow-multi-table-gribs", po::bool_switch(&options.allow_multi_table_gribs), "allow single grib file messages to be loaded to more than one radon table (in-place insert)")
		("metadata,m", po::value(&options.metadata_file_name), "write metadata of successful fields to this file (json)")
		("wait-timeout,w", po::value(&options.wait_timeout), "wait for file to to appear for this many seconds (default: 0)")
		;

	// clang-format on

	po::positional_options_description p;
	p.add("infile", -1);

	po::variables_map opt;
	po::store(po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run(), opt);

	po::notify(opt);

	if (opt.count("version"))
	{
		std::cout << "grid_to_radon compiled at " << __DATE__ << ' ' << __TIME__ << std::endl;
		return false;
	}

	if (opt.count("help"))
	{
		std::cout << desc;
		exit(0);
	}

	if (opt.count("infile") == 0)
	{
		std::cerr << "Expecting input file as parameter" << std::endl;
		std::cout << desc;
		return false;
	}

	options.ss_state_update = !no_ss_state_switch;
	options.directory_structure_check = !no_directory_structure_check_switch;

	if (max_failures >= -1)
	{
		options.max_failures = max_failures;
	}
	else
	{
		std::cerr << "Please specify maximum failures >= -1" << std::endl;
		return false;
	}

	if (max_skipped >= -1)
	{
		options.max_skipped = max_skipped;
	}
	else
	{
		std::cerr << "Please specify maximum skipped >= -1" << std::endl;
		return false;
	}

	himan::logger::MainDebugState = himan::kInfoMsg;

	if (verbose)
	{
		himan::logger::MainDebugState = himan::kDebugMsg;
	}

	switch (logLevel)
	{
		default:
			std::cerr << "Invalid debug level: " << logLevel << std::endl;
			exit(1);
		case -1:
			break;
		case 0:
			himan::logger::MainDebugState = himan::kFatalMsg;
			break;
		case 1:
			himan::logger::MainDebugState = himan::kErrorMsg;
			break;
		case 2:
			himan::logger::MainDebugState = himan::kWarningMsg;
			break;
		case 3:
			himan::logger::MainDebugState = himan::kInfoMsg;
			break;
		case 4:
			himan::logger::MainDebugState = himan::kDebugMsg;
			break;
		case 5:
			himan::logger::MainDebugState = himan::kTraceMsg;
			break;
	}

	if (options.wait_timeout > 0 && options.wait_timeout < 10)
	{
		logr.Warning("wait_timeout minimum value is 10, changing");
		options.wait_timeout = 10;
	}
	return true;
}

// std::quoted is c++14
std::string quoted(const std::string& v)
{
	return fmt::format("\"{}\"", v);
}

std::string RecordToJSON(const grid_to_radon::record& rec)
{
	// If this gets any more complicated an actual JSON
	// library should be used?

	// clang-format off
	std::string json = fmt::format("{{ {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {}, {} : {} }}",
		quoted("schema_name"), quoted(rec.schema_name),
		quoted("table_name"), quoted(rec.table_name),
		quoted("file_name"), quoted(rec.file_name),
		quoted("file_type"), rec.file_type,
		quoted("geometry_name"), quoted(rec.geometry_name),
		quoted("producer_id"), rec.producer.Id(),
		quoted("forecast_type_id"), rec.ftype.Type(),
		quoted("forecast_type_value"), rec.ftype.Value(),
		quoted("analysis_time"), quoted(rec.ftime.OriginDateTime().ToSQLTime()),
		quoted("forecast_period"), quoted(rec.ftime.Step().String("%h:%02M:%02S")),
		quoted("level_id"), rec.level.Type(),
		quoted("level_value"), rec.level.Value(),
		quoted("level_value2"), rec.level.Value2(),
		quoted("param_name"), quoted(rec.param.Name()));
	// clang-format on
	return json;
}

void WriteMetadata(const grid_to_radon::records& recs)
{
	if (options.metadata_file_name.empty())
	{
		return;
	}

	const std::string VERSION = "20210505";
	std::ofstream out(options.metadata_file_name);

	out << "{\n  " << quoted("version") << " : " << quoted(VERSION) << ", " << quoted("records") << " : [";

	size_t i = 0;
	for (const auto& rec : recs)
	{
		out << "    " << RecordToJSON(rec);
		if (++i != recs.size())
			out << ",\n";
	}

	out << "  ]\n}";
	out.close();

	logr.Info(fmt::format("Wrote metadata to '{}'", options.metadata_file_name));
}

bool file_exists(const std::string& file)
{
	auto exists = [&](const std::string& file_)
	{
		if (options.s3 == false)
		{
			return std::filesystem::exists(file_);
		}
		return himan::s3::Exists(file_);
	};

	if (options.wait_timeout == 0)
	{
		return exists(file);
	}

	namespace ch = std::chrono;

	const auto start = std::chrono::system_clock::now();

	// times are casted to milliseconds, otherwise wait loop might be ran one extra time
	// because time are truncated to second
	const ch::milliseconds wait_timeout(1000 * options.wait_timeout);

	// sleep time is 10% of wait time, between 10..60 seconds
	ch::milliseconds sleep_time =
	    ch::milliseconds(1000 * std::max(10, std::min(60, static_cast<int>(options.wait_timeout / 10))));

	ch::milliseconds cum_sleep(0);

	while (ch::duration_cast<ch::milliseconds>(ch::system_clock::now() - start) <= wait_timeout)
	{
		if (exists(file))
		{
			return true;
		}
		sleep_time = std::min(sleep_time, wait_timeout - cum_sleep);
		cum_sleep += sleep_time;
		logr.Info(fmt::format("File {} not present, waiting {} of {}", file, ch::duration_cast<ch::seconds>(cum_sleep),
		                      ch::duration_cast<ch::seconds>((wait_timeout))));
		std::this_thread::sleep_for(sleep_time);
	}

	// one more chance for file to appear
	return exists(file);
}

int main(int argc, char** argv)
{
	if (!parse_options(argc, argv))
	{
		return 1;
	}

	int retval = 0;

	grid_to_radon::records all_records;

	for (const std::string& infile : options.infile)
	{
		const bool isLocalFile = (infile.substr(0, 5) != "s3://");

		if (!isLocalFile)
		{
			options.s3 = true;
		}

		if (infile != "-" && file_exists(infile) == false)
		{
			logr.Error(fmt::format("Input file '{}' does not exist", infile));
			retval = 1;
			continue;
		}

		uintmax_t fileSize = 0;

		if (options.s3)
		{
			fileSize = himan::s3::ObjectSize(infile);
		}
		else
		{
			fileSize = std::filesystem::file_size(infile);
		}

		logr.Info(
		    fmt::format("Reading file '{}' (size: {:.1f}MB)", infile, static_cast<double>(fileSize) / 1024.0 / 1024.0));

		himan::HPFileType type = himan::kUnknownFile;

		if (options.netcdf == false && options.grib == false && options.geotiff == false)
		{
			type = himan::util::FileType(infile);
		}

		if (type == himan::kNetCDF || options.netcdf)
		{
			logr.Debug("File '" + infile + "' is NetCDF");

			if (options.in_place_insert)
			{
				logr.Error("In-place insert not possible for netcdf");
				retval = 1;
				continue;
			}
			else if (options.s3)
			{
				logr.Error("s3 loading not possible for netcdf");
				retval = 1;
				continue;
			}

			grid_to_radon::NetCDFLoader ncl;
			const auto ret = ncl.Load(infile);
			retval = static_cast<int>(!ret.first);
			all_records.insert(std::end(all_records), std::begin(ret.second), std::end(ret.second));
		}
		else if (type == himan::kGRIB1 || type == himan::kGRIB2 || type == himan::kGRIB || options.grib)
		{
			logr.Debug("File '" + infile + "' is GRIB");

			std::pair<bool, std::vector<grid_to_radon::record>> ret;

			if (options.s3)
			{
				grid_to_radon::S3GribLoader ldr;
				ret = ldr.Load(infile);
			}
			else
			{
				grid_to_radon::GribLoader ldr;
				ret = ldr.Load(infile);
			}

			retval = static_cast<int>(!ret.first);
			all_records.insert(std::end(all_records), std::begin(ret.second), std::end(ret.second));
		}
		else if (type == himan::kGeoTIFF || options.geotiff)
		{
			logr.Debug(fmt::format("File '{}' is GeoTIFF", infile));

			if (options.producer == 0)
			{
				logr.Error("Producer id must be specified with -p");
				retval = 1;
				continue;
			}
			if (options.analysistime.empty())
			{
				logr.Error("Analysis time must be specified with -a");
				retval = 1;
				continue;
			}

			grid_to_radon::GeoTIFFLoader g;

			options.in_place_insert = true;

			const auto ret = g.Load(infile);
			retval = static_cast<int>(!ret.first);
			all_records.insert(std::end(all_records), std::begin(ret.second), std::end(ret.second));
		}
		else
		{
			logr.Error(fmt::format("Unrecognized file type for '{}' and none of: -n -g -G defined", infile));
			retval = 1;
		}

		// early exit if needed
		if (retval != 0)
		{
			WriteMetadata(all_records);
			return retval;
		}
	}

	WriteMetadata(all_records);
	return retval;
}
