#include <iostream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>
#include "unistd.h"

#include "NetCDFLoader.h"
#include "GribLoader.h"
#include "GribIndexLoader.h"
#include "options.h"

Options options;

bool parse_options(int argc, char * argv[])
{
  namespace po = boost::program_options;
  namespace fs = boost::filesystem;

  po::options_description desc("Allowed options");

  bool radon_switch = false;
  bool neons_switch = false;
  int max_failures = -1;
  int max_skipped = -1;
  
  desc.add_options()
    ("help,h","print out help message")
    ("verbose,v",po::bool_switch(&options.verbose),"set verbose mode on")
    ("netcdf,n",po::bool_switch(&options.netcdf),"force netcdf mode on")
    ("grib,g",po::bool_switch(&options.grib),"force grib mode on")
    ("index",po::bool_switch(&options.index),"force grib index mode on")
    ("index-keys",po::value(&options.keys),"define keys for file indexing, using grib_api notation")
    ("version,V","display version number")
    ("infile,i",po::value(&options.infile),"input file")
    ("center,c",po::value(&options.center),"force center id")
    ("process,p",po::value(&options.process),"force process id")
    ("analysistime,a",po::value(&options.analysistime),"force analysis time")
    ("parameters,P",po::value(&options.parameters),"accept these parameters, comma separated list")
    ("level,L",po::value(&options.level),"force level (only nc)")
    ("leveltypes,l",po::value(&options.leveltypes),"accept these leveltypes, comma separated list")
    ("use-level-value",po::bool_switch(&options.use_level_value),"use level value instead of index")
    ("use-inverse-level-value",po::bool_switch(&options.use_inverse_level_value),"use inverse level value instead of index")
    ("max-failures", po::value(&max_failures), "maximum number of allowed loading failures (grib) -1 = \"don't care\"")
    ("max-skipped", po::value(&max_skipped), "maximum number of allowed skipped messages (grib) -1 = \"don't care\"")
    ("dry-run", po::bool_switch(&options.dry_run), "dry run (no changes made to database or disk), show all sql statements")
    ("threads,j", po::value(&options.threadcount), "number of threads to use. only applicable to grib")
    ("neons,N", po::bool_switch(&neons_switch), "use only neons database")
    ("radon,R", po::bool_switch(&radon_switch), "use only radon database")
    ;

  // Examples
  // bdap_load_file_ng -a 2012062600 -p 151 --use-inverse-level-value -L HEIGHT -v file.nc

  po::positional_options_description p;
  p.add("infile",1);

  po::variables_map opt;
  po::store(po::command_line_parser(argc,argv)
                        .options(desc)
                        .positional(p)
                        .run(),
                        opt);

  po::notify(opt);

  if(opt.count("version")) 
  {

    std::cout << "grid_to_neons compiled at "
              << __DATE__
              << ' '
              << __TIME__
              << std::endl;
    return false;
  }

  if(opt.count("help")) 
  {
    std::cout << desc;
    return false;
  }

  if(opt.count("infile") == 0) 
  {
    std::cerr << "Expecting input file as parameter" << std::endl;
    std::cout << desc;
    return false;
  }

  if(!fs::exists(options.infile)) 
  {
	  std::cerr << "Input file '"+options.infile+"' does not exist" << std::endl;
	  return false;
  }

  if (radon_switch && neons_switch)
  {
	  std::cerr << "Both neons and radon options cannot be specified" << std::endl;
	  return false;
  }
  else if (radon_switch)
  {
	  options.neons = false;
  }
  else if (neons_switch)
  {
	  options.radon = false;
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

int main(int argc, char ** argv)
{

  if (!parse_options(argc, argv))
    return 1;

  std::string extension = boost::filesystem::path(options.infile).extension().string();

  if (extension == ".nc" || options.netcdf)
  {
    if(options.verbose)
      std::cout << "Treating file '" << options.infile << "' as NetCDF" << std::endl;

    NetCDFLoader ncl;

    if (!ncl.Load(options.infile)) 
    {
      std::cerr << "Load failed" << std::endl;
      return 1;
    }


  }
  else if (extension == ".grib" || extension == ".grib2" || options.grib)
  {

    if(options.verbose)
      std::cout << "Treating file '" << options.infile << "' as GRIB" << std::endl;

    GribLoader g;

    if (!g.Load(options.infile)) 
    {
      std::cerr << "Load failed" << std::endl;
      return 1;
    }

  } 
  else if (options.index)
  {
    if(options.verbose)
      std::cout << "Treating file '" << options.infile << "' as GRIB using index" << std::endl;

    GribIndexLoader i;

    if (!i.Load(options.infile,options.keys))
    {
      std::cerr << "Load failed" << std::endl;
      return 1;
    }

  }
  else 
  {
    std::cerr << "Unable to determine file type for file '" << options.infile << "'" << std::endl
              << "Use switch -n or -g to force file type" << std::endl;
  }

  return 0;

}
