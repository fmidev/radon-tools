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
#include <thread>
#include <cstdatomic>

#ifdef DEBUG
timespec start_ts, stop_ts;
#endif

extern Options options;

using namespace std;

void Process(unique_ptr<NFmiGribMessage> message);

vector<string> parameters;
vector<string> levels;
  
atomic<int> success(0);
atomic<int> failed(0);

mutex distMutex;
  
GribLoader::GribLoader() {}
GribLoader::~GribLoader() {}

bool GribLoader::Load(const string &theInfile) 
{

  itsReader.Open(theInfile);

  // Read all message from file

  string levelString = options.level;

  if (!levelString.empty()) 
  {
    boost::split(levels, levelString, boost::is_any_of(","), boost::token_compress_on);
  }

  string paramString = options.parameters;

  if (!paramString.empty()) 
  {
    boost::split(parameters, paramString, boost::is_any_of(","), boost::token_compress_on);
  }

  vector<thread> threadgroup;
  
  for (short i = 0; i < options.threadcount; i++)
  {
	  threadgroup.push_back(thread(&GribLoader::Run, this, i));
  }
  
  for (unsigned short i = 0; i < threadgroup.size(); i++)
  {
	  threadgroup[i].join();
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

bool CopyMetaData(BDAPLoader& databaseLoader, fc_info &g, const NFmiGribMessage &message) 
{

  g.centre = message.Centre();
  g.ednum = message.Edition();

  g.param = message.ParameterNumber();
  g.levtype = message.LevelType();

  g.process = message.Process();

  if (options.process != 0)
    g.process = options.process;

  // Default to deterministic forecast
  
  g.forecast_type_id = 1;
  
  if (g.ednum == 1) 
  {
    g.filetype = "grib";

    g.novers = message.Table2Version();
    g.timeRangeIndicator = message.TimeRangeIndicator();
    
    g.parname = databaseLoader.NeonsDB().GetGridParameterName(g.param, g.novers, g.novers, g.timeRangeIndicator, g.levtype);
    g.levname = databaseLoader.NeonsDB().GetGridLevelName(g.param, g.levtype, g.novers, g.novers);

    if (g.parname.empty())
    {
      if (options.verbose)
      {
        cerr << "Parameter name not found for table2Version " << g.novers << ", number " << g.param << ", time range indicator " << g.timeRangeIndicator << " level type " << g.levtype << endl;
      }

      return false;
    }
  }
  else 
  {
    g.filetype = "grib2";

    g.timeRangeIndicator = 0;

    g.parname = databaseLoader.NeonsDB().GetGridParameterNameForGrib2(g.param, message.ParameterCategory(), message.ParameterDiscipline(), g.process);
    g.levname = databaseLoader.NeonsDB().GetGridLevelName(g.levtype, g.process);

    g.category = message.ParameterCategory();
    g.discipline = message.ParameterDiscipline();

    if (g.parname.empty())
    {
      if (options.verbose)
      {
        cerr << "Parameter name not found for category " << message.ParameterCategory() << ", discipline " << message.ParameterDiscipline() << " number " << g.param << endl;
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

  g.year = message.Year();
  g.month = message.Month();
  g.day = message.Day();
  g.hour = message.Hour();
  g.minute = message.Minute();

  g.ni = message.SizeX();
  g.nj = message.SizeY();

  g.lat = message.Y0() * 1000;
  g.lon = message.X0() * 1000;

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

  g.gridtype = message.GridType();
  switch (message.NormalizedGridType()) 
  {
    case 0: // ll
      g.di = message.iDirectionIncrement();
      g.dj = message.jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "ll";
      break;

    case 10: // rll
      g.di = message.iDirectionIncrement();
      g.dj = message.jDirectionIncrement();
      g.di *= 1000;
      g.dj *= 1000;
      g.grtyp = "rll";
      break;

    case 5: // ps, ei tarkoita puoluetta
      g.di = message.XLengthInMeters();
      g.dj = message.YLengthInMeters();
      g.grtyp = "ps";
      break;

    default:
      cerr << "Invalid geometry for GRIB: " << message.NormalizedGridType() << ", only latlon, rotated latlon and polster are supported" << endl;
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

  g.level1 = message.LevelValue();
  g.level2 = 0;

  g.lvl1_lvl2 = g.level1 + 1000 * g.level2;
  
  // Rewritten EPS logic

  if (g.ednum == 1)
  {
    long definitionNumber = 0;

    if (message.KeyExists("localDefinitionNumber"))
    {
      definitionNumber = message.LocalDefinitionNumber();
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
        long definitionType = message.Type();
        long perturbationNumber = message.PerturbationNumber();

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

      long typeOfGeneratingProcess = message.TypeOfGeneratingProcess();

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

          long typeOfEnsemble = message.TypeOfEnsembleForecast();
          long perturbationNumber = message.PerturbationNumber();

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
 
  g.stepType = message.TimeRangeIndicator();
  g.timeUnit = message.UnitOfTimeRange();

  g.startstep = message.NormalizedStep(false, false);
  g.endstep = message.NormalizedStep(true, false);
  g.step = g.endstep;
  
  g.fcst_per = message.NormalizedStep(true, true);

  return true;
}

void GribLoader::Run(short threadId)
{
  printf("Thread %d started\n", threadId);
  unique_ptr<NFmiGribMessage> myMessage;
  
  while(myMessage = DistributeMessages())
  {
    Process(move(myMessage));
  }
  
  printf("Thread %d stopped\n", threadId);
  
}

unique_ptr<NFmiGribMessage> GribLoader::DistributeMessages()
{
  lock_guard<mutex> lock(distMutex);
  while (itsReader.NextMessage()) 
  {
    if (options.verbose)
    {
      cout << "Message " << itsReader.CurrentMessageIndex() << ": ";
    }

    return itsReader.CloneMessage();

  }

  return unique_ptr<NFmiGribMessage> ();
}

void Process(unique_ptr<NFmiGribMessage> message)
{
   fc_info g;
   BDAPLoader databaseLoader;

   timespec start_ms_ts, stop_ms_ts;
   
   if (options.verbose)
   {
     clock_gettime(CLOCK_REALTIME, &start_ms_ts);
   }
    /*
     * Read metadata from grib msg
     */

#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &start_ts);
#endif

    if (!CopyMetaData(databaseLoader, g, *message))
    {
      return;
    }

    if (parameters.size() > 0) 
    {
      if (std::find(parameters.begin(), parameters.end(), g.parname) == parameters.end()) 
      {

        if (options.verbose)
          cout << "Skipping parameter " << g.parname << endl;

        return;
      }
    }

    if (levels.size() > 0) 
    {
      if (std::find(levels.begin(), levels.end(), g.levname) == levels.end()) 
      {

        if (options.verbose)
          cout << "Skipping level " << g.levname << endl;

        return;
      }
    }

	
#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &stop_ts);
    size_t start = static_cast<size_t> (start_ts.tv_sec*1000000000 + start_ts.tv_nsec);
    size_t stop =  static_cast<size_t> (stop_ts.tv_sec*1000000000 + stop_ts.tv_nsec);
    cerr << "grib reading: " << (stop - start) / 1000 / 1000 << " ms" << endl;
#endif

    if (options.verbose)
      cout << "Parameter: " << g.parname << " at level " << g.levname << " " << g.lvl1_lvl2 << endl;

    string theFileName = databaseLoader.REFFileName(g);

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

#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &start_ts);
#endif

    if (!options.dry_run)
    {
      if (!message->Write(theFileName, false))
      {
        failed++;
        return;
      }
    }

#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &stop_ts);
    start = static_cast<size_t> (start_ts.tv_sec*1000000000 + start_ts.tv_nsec);
    stop =  static_cast<size_t> (stop_ts.tv_sec*1000000000 + stop_ts.tv_nsec);
    cerr << "write to disk: " << (stop - start) / 1000 / 1000 << " ms" << endl;
#endif

    /*
     * Update new file information to database
     */

	if (options.neons)
	{
#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &start_ts);
#endif

      if (!databaseLoader.WriteAS(g))
      {
        failed++;
        return;
      }
	  
#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &stop_ts);
    start = static_cast<size_t> (start_ts.tv_sec*1000000000 + start_ts.tv_nsec);
    stop =  static_cast<size_t> (stop_ts.tv_sec*1000000000 + stop_ts.tv_nsec);
    cerr << "write to neons: " << (stop - start) / 1000 / 1000 << " ms" << endl;
#endif
	}

	if (options.radon)
	{
#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &start_ts);
#endif
		databaseLoader.WriteToRadon(g);

#ifdef DEBUG
    clock_gettime(CLOCK_REALTIME, &stop_ts);
    start = static_cast<size_t> (start_ts.tv_sec*1000000000 + start_ts.tv_nsec);
    stop =  static_cast<size_t> (stop_ts.tv_sec*1000000000 + stop_ts.tv_nsec);
    cerr << "write to radon: " << (stop - start) / 1000 / 1000 << " ms" << endl;
#endif

	}

    success++;

    if (options.verbose)
    {
      clock_gettime(CLOCK_REALTIME, &stop_ms_ts);
      size_t start_ms = static_cast<size_t> (start_ms_ts.tv_sec*1000000000 + start_ms_ts.tv_nsec);
      size_t stop_ms = static_cast<size_t> (stop_ms_ts.tv_sec*1000000000 + stop_ms_ts.tv_nsec);
      cout << "Message loaded in " << (stop_ms - start_ms) / 1000 / 1000 << " ms" << endl;
    }
 	
}
