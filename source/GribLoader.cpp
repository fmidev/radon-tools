#include "GribLoader.h"
#include <stdlib.h>
#include <sstream>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include "NFmiNeonsDB.h"
#include "NFmiRadonDB.h"
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

    itsDatabaseLoader.WriteToRadon(g);

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

  g.centre = reader.Message().Centre();
  g.ednum = reader.Message().Edition();

  g.param = reader.Message().ParameterNumber();
  g.levtype = reader.Message().LevelType();

  g.process = reader.Message().Process();

  if (options.process != 0)
    g.process = options.process;

  if (g.ednum == 1) 
  {
    g.filetype = "grib";

    g.novers = reader.Message().Table2Version();
    g.timeRangeIndicator = reader.Message().TimeRangeIndicator();
    
    g.parname = NFmiNeonsDB::Instance().GetGridParameterName(g.param, g.novers, g.novers, g.timeRangeIndicator);
    g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.param, g.levtype, g.novers, g.novers);

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

    g.parname = NFmiNeonsDB::Instance().GetGridParameterNameForGrib2(g.param, reader.Message().ParameterCategory(), reader.Message().ParameterDiscipline(), g.process);
    g.levname = NFmiNeonsDB::Instance().GetGridLevelName(g.levtype, g.process);

    g.category = reader.Message().ParameterCategory();
    g.discipline = reader.Message().ParameterDiscipline();

    if (g.parname.empty())
    {
      if (options.verbose)
      {
        cerr << "Parameter name not found for category " << reader.Message().ParameterCategory() << ", discipline " << reader.Message().ParameterDiscipline() << " number " << g.param << endl;
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

  g.year = reader.Message().Year();
  g.month = reader.Message().Month();
  g.day = reader.Message().Day();
  g.hour = reader.Message().Hour();
  g.minute = reader.Message().Minute();

  g.ni = reader.Message().SizeX();
  g.nj = reader.Message().SizeY();

  //g.di = reader.Message().iDirectionIncrement();
  //g.dj = reader.Message().jDirectionIncrement();

  g.lat = reader.Message().Y0() * 1000;
  g.lon = reader.Message().X0() * 1000;

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

  g.gridtype = reader.Message().GridType();
  
  switch (reader.Message().NormalizedGridType()) 
  {
    case 0: // ll
      g.di = reader.Message().iDirectionIncrement();
      g.dj = reader.Message().jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "ll";
      break;

    case 10: // rll
      g.di = reader.Message().iDirectionIncrement();
      g.dj = reader.Message().jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "rll";
      break;

    case 5: // ps, ei tarkoita puoluetta
      g.di = reader.Message().XLengthInMeters();
      g.dj = reader.Message().YLengthInMeters();
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

  g.level1 = reader.Message().LevelValue();
  g.level2 = 0;

  g.lvl1_lvl2 = g.level1 + 1000 * g.level2;

#if 0

  g.locdef = 0; 
  // if exists, otherwise zero
  if (reader.Message().KeyExists("localDefinitionNumber"))
  {
    g.locdef = reader.Message().LocalDefinitionNumber();  
  }

  if (g.ednum == 1) 
  {

     if (g.locdef == 1 || g.locdef == 2 || g.locdef == 5 || g.locdef == 15 || g.locdef == 16 || g.locdef == 30) 
     {
       g.ldeftype = reader.Message().Type();
       g.ldefnumber = reader.Message().PerturbationNumber();
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
         g.ldeftype = reader.Message().TypeOfEnsembleForecast();
         g.ldefnumber = reader.Message().PerturbationNumber();
         break;

       case 2:
       case 12:
         g.ldeftype = reader.Message().DerivedForecast();
         g.ldefnumber = reader.Message().NumberOfForecastsInTheEnsemble();
         break;

       case 3:
       case 4:
       case 13:
       case 14:
         g.ldeftype = reader.Message().DerivedForecast();
         g.ldefnumber = reader.Message().ClusterIdentifier();
         break;

       case 5:
       case 9:
         g.ldeftype = reader.Message().ForecastProbabilityNumber();
         g.ldefnumber = reader.Message().ProbabilityType();
         break;

       case 6:
       case 10:
         g.ldeftype = reader.Message().PercentileValue();
         g.ldefnumber = 0;
         break;

       case 8:
         g.ldeftype = reader.Message().NumberOfTimeRange();
         g.ldefnumber = reader.Message().TypeOfTimeIncrement();
         g.step = reader.Message().EndStep();
         break;
     }
   }

  if (g.locdef > 0)
    g.eps_specifier = boost::lexical_cast<string> (g.locdef) + "_" + boost::lexical_cast<string> (g.ldeftype) + "_" + boost::lexical_cast<string> (g.ldefnumber);
  else
    g.eps_specifier = "0";
#endif
  
  // Rewritten EPS logic

  if (g.ednum == 1)
  {
    long definitionNumber = 0;

    if (reader.Message().KeyExists("localDefinitionNumber"))
    {
      definitionNumber = reader.Message().LocalDefinitionNumber();
    }

    // EC uses local definition number in Grib1
    // http://old.ecmwf.int/publications/manuals/d/gribapi/fm92/grib1/show/local/
  
    switch (definitionNumber)
    {
      case 0:
        // no local definition --> deterministic
        g.forecast_type_id = 1;
        g.forecast_type_value = kFloatMissing;
        break;

      case 1:
        // MARS labeling or ensemble forecast data
      {
        long definitionType = reader.Message().Type();
        long perturbationNumber = reader.Message().PerturbationNumber();

        switch (definitionType)
        {
          case 10:
            // cf -- control forecast
            g.forecast_type_id = 4;
            g.forecast_type_value = kFloatMissing;
            break;
          case 11:
            // pf -- perturbed forecast
            g.forecast_type_id = 3;
            g.forecast_type_value = perturbationNumber;
            break;
          default:
            cerr << "Unknown localDefinitionType: " << definitionType << endl;
            break;
        }
		break;
      }
      default:
        cerr << "Unknown localDefinitionNumber: " << definitionNumber << endl;
        break;
    }
  }
  else
  {
      // grib2

      long typeOfGeneratingProcess = reader.Message().TypeOfGeneratingProcess();

      switch (typeOfGeneratingProcess)
      {
        case 0:
          // Analysis
          g.forecast_type_id = 2;
          g.forecast_type_value = kFloatMissing;
          break;

        case 2:
          // deterministic
          g.forecast_type_id = 1;
          g.forecast_type_value = kFloatMissing;
          break;

        case 4:
          // eps
        {

          long typeOfEnsemble = reader.Message().TypeOfEnsembleForecast();
          long perturbationNumber = reader.Message().PerturbationNumber();

          switch (typeOfEnsemble)
          {
            case 0:
            case 1:
              // control forecast
              g.forecast_type_id = 4;
              g.forecast_type_value = kFloatMissing;
              break;

            case 2:
            case 3:
            case 192:
              // perturbed forecast
              g.forecast_type_id = 3;
              g.forecast_type_value = perturbationNumber;
              break;

            default:
              cerr << "Unknown type of ensemble: " << typeOfEnsemble << endl;
              break;
	      }
          break;
        }

	    default:
		  cerr << "Unknown type of generating process: " << typeOfGeneratingProcess << endl;
		  break;
	  }
  }
    
  // Force eps_specifier to 0 because for some EC data it gets value other than zero from
  // the if-chain above and that does not work well with hilake.
  // When the issue with logic above is fixed, this hotfix can be removed. In the mean while
  // it might not be safe to load actual EPS data with this program.

  g.eps_specifier = "0";
 
  g.stepType = reader.Message().TimeRangeIndicator();
  g.timeUnit = reader.Message().UnitOfTimeRange();

  g.startstep = reader.Message().NormalizedStep(false, false);
  g.endstep = reader.Message().NormalizedStep(true, false);
  g.step = g.endstep;
  
  g.fcst_per = reader.Message().NormalizedStep(true, true);

  return true;
}
