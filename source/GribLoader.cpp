#include "GribLoader.h"
#include <stdlib.h>
#include <sstream>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include "NFmiNeonsDB.h"
#ifdef NEON2
#include "NFmiNeon2DB.h"
#endif
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include "options.h"

extern Options options;

using namespace std;

GribLoader::GribLoader() : itsDatabaseLoader()
{
}

GribLoader::~GribLoader() 
{
}

bool GribLoader::Load(const string &theInfile) 
{

  NFmiGrib reader(theInfile);

  // Read all message from file

  vector<string> levels;

  string levelString = options.level;

  if (!levelString.empty()) 
  {
    boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
  }

  vector<string> parameters;

  string paramString = options.parameters;

  if (!paramString.empty()) 
  {
    boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
  }

  map<string,string> pskip;
  map<string,string> lskip;
  int success = 0;
  int failed = 0;

  while (reader.NextMessage()) 
  {

    if (options.verbose)
      cout << "Message " << reader.CurrentMessageIndex() << ": ";

    fc_info g;

    /*
     * Read metadata from grib msg
     */

    if (!CopyMetaData(g, reader) || pskip.count(g.parname) > 0 || lskip.count(g.levname) > 0)
    {
      if (options.verbose)
       cout << "Skipping due to cached information" << endl;

      continue;
	}

    if (parameters.size() > 0) 
    {
      if (std::find(parameters.begin(), parameters.end(), g.parname) == parameters.end()) 
      {

        if (options.verbose)
          cout << "Skipping parameter " << g.parname << endl;

        pskip[g.parname] = 1;

        continue;
      }
    }

    if (levels.size() > 0) 
    {
      if (std::find(levels.begin(), levels.end(), g.levname) == levels.end()) 
      {

        if (options.verbose)
          cout << "Skipping level " << g.levname << endl;

        lskip[g.levname] = 1;

        continue;
      }
    }

    if (options.verbose)
      cout << "Parameter: " << g.parname << " at level " << g.levname << " " << g.lvl1_lvl2 << endl;

    string theFileName = itsDatabaseLoader.REFFileName(g);

    if (theFileName.empty())
      exit(1);

    g.filename = theFileName;

    namespace fs = boost::filesystem;

    fs::path pathname(theFileName);

    if (!fs::is_directory(pathname.parent_path())) 
    {

      // Create directory

      if (options.verbose)
        cout << "Creating directory " << pathname.parent_path().string() << endl;

      if (!options.dry_run)
        fs::create_directories(pathname.parent_path());
    }

    /*
     * Write grib msg to disk with unique filename.
     */

    if (!options.dry_run)
    {
      if (!reader.WriteMessage(theFileName))
      {
        failed++;
        return false;
      }
    }

    /*
     * Update new file information to database
     */

    if (!itsDatabaseLoader.WriteAS(g))
    {
      failed++;
      return false;
    }
#ifdef NEON2
	itsDatabaseLoader.WriteToNeon2(g);
#endif
    success++;

  }

  cout << "Loaded " << success << " fields successfully\n";

  if (failed > 0)
  {
    cout << "Error occured with " << failed << " fields" << endl;
  }

  return true;
}



/*
 * CopyMetaData()
 *
 * Read all necessary metadata from a grib message. Structure of function
 * copied from PutGribMsgToNeons_api() (putgribmsgtoneons_api.c:87)
 */

bool GribLoader::CopyMetaData(fc_info &g, NFmiGrib &reader) 
{

  g.centre = reader.Message()->Centre();
  g.ednum = reader.Message()->Edition();

  g.param = reader.Message()->ParameterNumber();
  g.levtype = reader.Message()->LevelType();

  g.process = reader.Message()->Process();

  if (options.process != 0)
    g.process = options.process;

  if (g.ednum == 1) 
  {
    g.filetype = "grib";

    g.novers = reader.Message()->Table2Version();
    g.timeRangeIndicator = reader.Message()->TimeRangeIndicator();
    
    g.parname = NFmiNeonsDB::Instance().GetGridParameterName(g.param, g.novers, g.novers, g.timeRangeIndicator);
    g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.param, g.levtype, g.novers, g.novers);

#ifdef NEON2

	map<string,string> r = NFmiNeon2DB::Instance().ProducerFromGrib1(g.centre, g.process);

	g.producer_id = boost::lexical_cast<long> (r["id"]);
	
	map<string,string> l = NFmiNeon2DB::Instance().LevelFromGrib1(g.producer_id, g.levtype);

	if (l.empty())
	{
		cerr << "Level not found from neon2\n";
	}

	map<string,string> p = NFmiNeon2DB::Instance().ParameterFromGrib1(g.producer_id, g.novers, g.param, g.timeRangeIndicator, boost::lexical_cast<long> (l["id"]), reader.Message()->LevelValue());

	if (p.empty())
	{
		cerr << "Parameter not found from neon2\n";
	}


#endif

    if (g.parname.empty())
    {
      if (options.verbose)
      {
        cerr << "Parameter name not found for table2Version " << g.novers << ", number " << g.param << ", time range indicator " << g.timeRangeIndicator << endl;
      }

      return false;
    }
  }
  else 
  {
    g.filetype = "grib2";
	
    g.timeRangeIndicator = 0;

    g.parname = NFmiNeonsDB::Instance().GetGridParameterNameForGrib2(g.param, reader.Message()->ParameterCategory(), reader.Message()->ParameterDiscipline(), g.process);
    g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.levtype, g.process);

    if (g.parname.empty())
    {
      if (options.verbose)
      {
        cerr << "Parameter name not found for category " << reader.Message()->ParameterCategory() << ", discipline " << reader.Message()->ParameterDiscipline() << " number " << g.param << endl;
      }

    return false;
    }
  }

  if (g.levname.empty()) 
  {
    if (options.verbose)
    {
      cerr << "Level name not found for level " << g.levtype << endl;
    }
    return false;
  }

  g.year = reader.Message()->Year();
  g.month = reader.Message()->Month();
  g.day = reader.Message()->Day();
  g.hour = reader.Message()->Hour();
  g.minute = reader.Message()->Minute();

  g.ni = reader.Message()->SizeX();
  g.nj = reader.Message()->SizeY();

  //g.di = reader.Message()->iDirectionIncrement();
  //g.dj = reader.Message()->jDirectionIncrement();

  g.lat = reader.Message()->Y0() * 1000;
  g.lon = reader.Message()->X0() * 1000;

  // This is because we need to find the 
  // correct geometry from GRID_REG_GEOM in neons
  
  // GRIB2 longitudes --> GRIB1 
  if (g.ednum == 2 && (g.lon > 180000)) 
  {  
    g.lon -= 360000;
  }
  else if (g.ednum == 1 && g.lon == -180000 && g.centre != 86) 
  {
    g.lon += 360000;  // Area is whole globe, ECMWF special case
  } 

  g.gridtype = reader.Message()->GridType();
  
  switch (reader.Message()->NormalizedGridType()) 
  {
    case 0: // ll
      g.di = reader.Message()->iDirectionIncrement();
      g.dj = reader.Message()->jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "ll";
      break;

    case 10: // rll
      g.di = reader.Message()->iDirectionIncrement();
      g.dj = reader.Message()->jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "rll";
      break;

    case 5: // ps, ei tarkoita puoluetta
      g.di = reader.Message()->XLengthInMeters();
      g.dj = reader.Message()->YLengthInMeters();
      g.grtyp = "ps";
      break;

    default:
      cerr << "Invalid geometry for GRIB: only latlon, rotated latlon and polster are supported" << endl;
      return false;
      break;

  }

  stringstream ss;

  ss << g.year
     << setw(2)
     << setfill('0')
     << g.month
     << setw(2)
     << setfill('0')
     << g.day
     << setw(2)
     << setfill('0')
     << g.hour
     << "0000";

  g.base_date = ss.str();

  g.level1 = reader.Message()->LevelValue();
  g.level2 = 0;

  g.lvl1_lvl2 = g.level1 + 1000 * g.level2;

  g.locdef = 0; 
  // if exists, otherwise zero
  if (reader.Message()->KeyExists("localDefinitionNumber"))
  {
    g.locdef = reader.Message()->LocalDefinitionNumber();  
  }

  if (g.ednum == 1) 
  {

     if (g.locdef == 1 || g.locdef == 2 || g.locdef == 5 || g.locdef == 15 || g.locdef == 16 || g.locdef == 30) 
     {
       g.ldeftype = reader.Message()->DataType();
       g.ldefnumber = reader.Message()->PerturbationNumber();
     }
     else if (g.locdef == 19) 
     {
       g.ldefnumber = 0;
     }
   }
   else if (g.ednum == 2) 
   {

     switch (g.locdef) 
     {
       case 1:
       case 11:
         g.ldeftype = reader.Message()->TypeOfEnsembleForecast();
         g.ldefnumber = reader.Message()->PerturbationNumber();
         break;

       case 2:
       case 12:
         g.ldeftype = reader.Message()->DerivedForecast();
         g.ldefnumber = reader.Message()->NumberOfForecastsInTheEnsemble();
         break;

       case 3:
       case 4:
       case 13:
       case 14:
         g.ldeftype = reader.Message()->DerivedForecast();
         g.ldefnumber = reader.Message()->ClusterIdentifier();
         break;

       case 5:
       case 9:
         g.ldeftype = reader.Message()->ForecastProbabilityNumber();
         g.ldefnumber = reader.Message()->ProbabilityType();
         break;

       case 6:
       case 10:
         g.ldeftype = reader.Message()->PercentileValue();
         g.ldefnumber = 0;
         break;

       case 8:
         g.ldeftype = reader.Message()->NumberOfTimeRange();
         g.ldefnumber = reader.Message()->TypeOfTimeIncrement();
         g.step = reader.Message()->EndStep();
         break;
     }
   }

  if (g.locdef > 0)
    g.eps_specifier = boost::lexical_cast<string> (g.locdef) + "_" + boost::lexical_cast<string> (g.ldeftype) + "_" + boost::lexical_cast<string> (g.ldefnumber);
  else
    g.eps_specifier = "0";

  // Force eps_specifier to 0 because for some EC data it gets value other than zero from
  // the if-chain above and that does not work well with hilake.
  // When the issue with logic above is fixed, this hotfix can be removed. In the mean while
  // it might not be safe to load actual EPS data with this program.

  g.eps_specifier = "0";
 
  g.stepType = reader.Message()->TimeRangeIndicator();
  g.timeUnit = reader.Message()->UnitOfTimeRange();

  g.startstep = reader.Message()->NormalizedStep(false, false);
  g.endstep = reader.Message()->NormalizedStep(true, false);
  g.step = g.endstep;
  
  g.fcst_per = reader.Message()->NormalizedStep(true, true);

  return true;
}
