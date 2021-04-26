#include "unistd.h"
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>

#include "geotiffloader.h"
#include "gribloader.h"
#include "netcdfloader.h"
#include "options.h"
#include "s3gribloader.h"
#include <logger.h>
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
		("allow-multi-table-gribs", po::bool_switch(&options.allow_multi_table_gribs), "allow single grib file messages to be loaded to more than one radon table (in-place insert)");

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
		return false;
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

	return true;
}

int main(int argc, char** argv)
{
	if (!parse_options(argc, argv))
	{
		return 1;
	}

	int retval = 0;

	for (const std::string& infile : options.infile)
	{
		const bool isLocalFile = (infile.substr(0, 5) != "s3://");

		if (isLocalFile && infile != "-" && !boost::filesystem::exists(infile))
		{
			logr.Error("Input file '" + infile + "' does not exist");
			continue;
		}

		logr.Info("Reading file '" + infile + "'");

		if (isLocalFile == false)
		{
			if (options.grib == false)
			{
				logr.Error("Only grib files are supported with s3");
				continue;
			}

			grid_to_radon::S3GribLoader ldr;
			options.s3 = true;

			if (!ldr.Load(infile))
			{
				retval = 1;
			}

			continue;
		}
		else
		{
			options.s3 = false;
		}

		auto type = himan::util::FileType(infile);

		if (type == himan::kNetCDF || options.netcdf)
		{
			logr.Debug("File '" + infile + "' is NetCDF");

			if (options.in_place_insert)
			{
				logr.Error("In-place insert not possible for netcdf");
				retval = 1;
				continue;
			}

			options.ss_state_update = false;

			grid_to_radon::NetCDFLoader ncl;

			if (!ncl.Load(infile))
			{
				retval = 1;
			}
		}
		else if (type == himan::kGRIB1 || type == himan::kGRIB2 || type == himan::kGRIB || options.grib)
		{
			logr.Debug("File '" + infile + "' is GRIB");

			grid_to_radon::GribLoader g;

			if (!g.Load(infile))
			{
				retval = 1;
			}
		}
		else if (type == himan::kGeoTIFF)
		{
			logr.Debug("File '" + infile + "' is GeoTIFF");

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

			if (!g.Load(infile))
			{
				logr.Error("Load failed");
				retval = 1;
			}
		}
		else
		{
			logr.Error("Unrecognized file type for '" + infile + "'");
			retval = 1;
		}

		// early exit if needed
		if (retval != 0)
		{
			return retval;
		}
	}

	return retval;
}
