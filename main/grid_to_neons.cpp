#include <iostream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>
#include "unistd.h"

#include "NetCDFLoader.h"
#include "GribLoader.h"


struct Options
{
  Options();

  bool verbose;                 // -v
  bool netcdf;                  // -n
  bool grib;                    // -g
  unsigned int center;		// -c
  unsigned int process;         // -p
  std::string analysistime;     // -a
  std::string infile;
  std::string parameters;       // -P
  std::string level;            // -L
  bool use_level_value;         // --use-level-value
  bool use_inverse_level_value; // --use-inverse-level-value
  bool dry_run;                 // -d;
  std::string leveltypes;       // -l
};

Options::Options()
  : verbose(false)
  , netcdf(false)
  , grib(false)
  , center(86)
  , process(0)
  , analysistime("")
  , infile("")
  , parameters("")
  , level("")
  , use_level_value(false)
  , use_inverse_level_value(false)
  , dry_run(false)
  , leveltypes("")
{
}

bool parse_options(int argc, char * argv[], Options & options)
{
  namespace po = boost::program_options;
  namespace fs = boost::filesystem;

  po::options_description desc("Allowed options");

  desc.add_options()
    ("help,h","print out help message")
    ("verbose,v",po::bool_switch(&options.verbose),"set verbose mode on")
    ("netcdf,n",po::bool_switch(&options.netcdf),"force netcdf mode on")
    ("grib,g",po::bool_switch(&options.grib),"force grib mode on")
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
    ("dry-run", po::bool_switch(&options.dry_run), "dry run (no changes made to database or disk), show all sql statements")
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

    std::cout << "bdap_load_file_ng compiled at "
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

  return true;
}

int main(int argc, char ** argv)
{
  Options options;

  if (!parse_options(argc, argv, options))
    return 1;

  uid_t uid = getuid();

  if (uid != 1459) // weto
  {
    std::cerr << "This program must be run as user weto." << std::endl;
    return 1;
  }


  std::string extension = boost::filesystem::path(options.infile).extension().string();

  if (extension == ".nc" || options.netcdf)
  {

    if(options.verbose)
      std::cout << "Treating file '" << options.infile << "' as NetCDF" << std::endl;

    NetCDFLoader ncl;

    ncl.Verbose(options.verbose);

    if (options.process)
      ncl.Process(options.process);

    if (!options.analysistime.empty())
      ncl.AnalysisTime(options.analysistime);

    if (!options.parameters.empty())
      ncl.Parameters(options.parameters);

    if (!options.level.empty())
      ncl.Level(options.level);

    if (!options.leveltypes.empty())
      ncl.Level(options.leveltypes);

    ncl.UseLevelValue(options.use_level_value);
    ncl.UseInverseLevelValue(options.use_inverse_level_value);
    ncl.DryRun(options.dry_run);

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

    g.Verbose(options.verbose);
    g.DryRun(options.dry_run);

    if (!options.parameters.empty())
      g.Parameters(options.parameters);

    if (!options.leveltypes.empty())
      g.Level(options.leveltypes);

    if (options.process)
      g.Process(options.process);

    if (!g.Load(options.infile)) 
    {
      std::cerr << "Load failed" << std::endl;
      return 1;
    }

  } else 
  {
    std::cerr << "Unable to determine file type for file '" << options.infile << "'" << std::endl
              << "Use switch -n or -g to force file type" << std::endl;
  }

  return 0;

}
