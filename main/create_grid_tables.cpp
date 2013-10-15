#include <iostream>
#include <fstream>
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>
#include "unistd.h"

#include "GribCreate.h"

struct Options
{
  Options();

  bool verbose;                 // -v
  std::string date;
  std::string hour;       // -P
  bool dry_run;                 // -d;
};

Options::Options()
  : verbose(false)
  , date("")
  , hour("") 
  , dry_run(false)
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
    ("version,V","display version number")
    ("date,i",po::value(&options.date),"input file")
    ("hour,a",po::value(&options.hour),"force hour time")   
    ("dry-run", po::bool_switch(&options.dry_run), "dry run (no changes made to database or disk), show all sql statements")
    ;

  po::positional_options_description p;
  p.add("date",1);
  p.add("hour",2);

  po::variables_map opt;
  po::store(po::command_line_parser(argc,argv)
                        .options(desc)
                        .positional(p)
                        .run(),
                        opt);

  po::notify(opt);

  if(opt.count("version")) {

    std::cout << "create_grib_tables compiled at "
              << __DATE__
              << ' '
              << __TIME__
              << std::endl;
    return false;
  }

  if(opt.count("help")) {
    std::cout << desc;
    return false;
  }

  if(opt.count("date") == 0) {
    std::cerr << "Expecting date as parameter" << std::endl;
    std::cout << desc;
    return false;
  }

  if(opt.count("hour") == 0) {
    std::cerr << "Expecting hour as second parameter" << std::endl;
    std::cout << desc;
    return false;
  }

  return true;
}

int main(int argc, char ** argv)
{
  Options options;

  if (!parse_options(argc, argv, options))
  {  
    return 1;
  }
  else 
  {
      uid_t uid = getuid();

      if (uid != 1459) // weto
      {
        std::cerr << "This program must be run as user weto." << std::endl;
        return 1;
      }

      std::cout << "Creating tables for date: " << options.date << " and hour: " << options.hour << "\n";
      GribCreate g;
      g.DryRun(options.dry_run);

      if (!g.Create(options.date, options.hour)) 
      {
        return 1;
      }

  } 

  return 0;

}
