#include "unistd.h"
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>

#include "GribIndexLoader.h"
#include "GribLoader.h"
#include "NetCDFLoader.h"
#include "options.h"

Options options;

enum class filetype
{
	unknown = 0,
	grib,
	gribindex,
	netcdf
};

filetype FileType(const std::string& theFile);

bool parse_options(int argc, char* argv[])
{
	namespace po = boost::program_options;

	po::options_description desc("Allowed options");

	bool radon_switch = false;
	bool neons_switch = false;
	int max_failures = -1;
	int max_skipped = -1;

	desc.add_options()("help,h", "print out help message")("verbose,v", po::bool_switch(&options.verbose),
	                                                       "set verbose mode on")(
	    "netcdf,n", po::bool_switch(&options.netcdf), "force netcdf mode on")(
	    "grib,g", po::bool_switch(&options.grib), "force grib mode on")("index", po::bool_switch(&options.index),
	                                                                    "force grib index mode on")(
	    "index-keys", po::value(&options.keys), "define keys for file indexing, using grib_api notation")(
	    "version,V", "display version number")("infile,i", po::value<std::vector<std::string>>(&options.infile),
	                                           "input file")("center,c", po::value(&options.center), "force center id")(
	    "process,p", po::value(&options.process), "force process id")(
	    "analysistime,a", po::value(&options.analysistime), "force analysis time")(
	    "parameters,P", po::value(&options.parameters), "accept these parameters, comma separated list")(
	    "level,L", po::value(&options.level), "force level (only nc)")("leveltypes,l", po::value(&options.leveltypes),
	                                                                   "accept these leveltypes, comma separated list")(
	    "use-level-value", po::bool_switch(&options.use_level_value), "use level value instead of index")(
	    "use-inverse-level-value", po::bool_switch(&options.use_inverse_level_value),
	    "use inverse level value instead of index")(
	    "max-failures", po::value(&max_failures),
	    "maximum number of allowed loading failures (grib) -1 = \"don't care\"")(
	    "max-skipped", po::value(&max_skipped),
	    "maximum number of allowed skipped messages (grib) -1 = \"don't care\"")(
	    "dry-run", po::bool_switch(&options.dry_run),
	    "dry run (no changes made to database or disk), show all sql statements")(
	    "threads,j", po::value(&options.threadcount), "number of threads to use. only applicable to grib")(
	    "neons,N", po::bool_switch(&neons_switch), "use only neons database")("radon,R", po::bool_switch(&radon_switch),
	                                                                          "use only radon database");

	po::positional_options_description p;
	p.add("infile", -1);

	po::variables_map opt;
	po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), opt);

	po::notify(opt);

	if (opt.count("version"))
	{
		std::cout << "grid_to_neons compiled at " << __DATE__ << ' ' << __TIME__ << std::endl;
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

	if (radon_switch)
	{
		std::cout << "Switch -R is deprecated" << std::endl;
	}

	if (neons_switch)
	{
		std::cout << "Switch -N is deprecated" << std::endl;
	}

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

	if (options.grib && options.index)
	{
		std::cerr << "Note: option --index implies -g" << std::endl;
		options.grib = false;
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
		if (!boost::filesystem::exists(infile))
		{
			std::cerr << "Input file '" << infile << "' does not exist" << std::endl;
			continue;
		}

		filetype type = FileType(infile);

		if (type == filetype::netcdf || options.netcdf)
		{
			if (options.verbose) std::cout << "File '" << infile << "' is NetCDF" << std::endl;

			NetCDFLoader ncl;

			if (!ncl.Load(infile))
			{
				std::cerr << "Load failed" << std::endl;
				retval = 1;
			}
		}
		else if (type == filetype::grib || options.grib)
		{
			if (options.verbose) std::cout << "File '" << infile << "' is GRIB" << std::endl;

			GribLoader g;

			if (!g.Load(infile))
			{
				std::cerr << "Load failed" << std::endl;
				retval = 1;
			}
		}
		else if (type == filetype::gribindex || options.index)
		{
			if (options.verbose) std::cout << "File '" << infile << "' is GRIB index" << std::endl;

			GribIndexLoader i;

			if (!i.Load(infile, options.keys))
			{
				std::cerr << "Load failed" << std::endl;
				retval = 1;
			}
		}
		else
		{
			std::cerr << "Unable to determine file type for file '" << infile << "'" << std::endl
			          << "Use switch -n or -g to force file type" << std::endl;
		}
	}

	return retval;
}

filetype FileType(const std::string& theFile)
{
	// First check by extension since its cheap

	boost::filesystem::path p(theFile);

	const std::string ext = p.extension().string();

	if (ext == ".grib" || ext == ".grib2" || ext == ".grb")
	{
		return filetype::grib;
	}
	else if (ext == ".nc")
	{
		return filetype::netcdf;
	}
	else if (ext == ".idx")
	{
		return filetype::gribindex;
	}

	// Try the check the file header; CSV is not possible anymore

	std::ifstream f(theFile.c_str(), std::ios::in | std::ios::binary);

	constexpr long keywordLength = 4;

	char content[keywordLength];

	f.read(content, keywordLength);

	filetype ret = filetype::unknown;

	if (strncmp(content, "GRIB", 4) == 0)
	{
		ret = filetype::grib;
	}
	else if (strncmp(content, "CDF", 3) == 0)
	{
		ret = filetype::netcdf;
	}

	return ret;
}
