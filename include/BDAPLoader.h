#ifndef __BDAPLOADER__
#define __BDAPLOADER__

/*
 * class BDAPLoader
 *
 * Superclass for NetCDFLoader.
 */

#include <cstring>
#include <string>

class BDAPLoader 
{

  public:

    BDAPLoader();
    virtual ~BDAPLoader();

    virtual bool Load(const std::string &theInfile) = 0;

    bool Verbose();
    void Verbose(bool theVerbose);

    unsigned long Process();
    void Process(unsigned long theProcess);

    std::string Level();
    void Level(std::string theLevel);

    std::string AnalysisTime();
    void AnalysisTime(std::string theAnalysisTime);

    std::string Parameters();
    void Parameters(std::string theParameters);

    std::string LevelTypes();
    void LevelTypes(std::string theLevelTypes);

    void UseLevelValue(bool theValue);
    void UseInverseLevelValue(bool theValue);

    void DryRun(bool theDryRun);
    bool DryRun();

  protected:

    std::string itsUsername;
    std::string itsPassword;
    std::string itsDatabase;

    bool itsVerbose;

    long itsProcess;
    std::string itsAnalysisTime;
    std::string itsParameters;
    std::string itsLevel;
    std::string itsLevelTypes;

    std::string itsGeomName;
    std::string itsModelName;
    std::string itsModelType;
    std::string itsTypeSmt;
    std::string itsDsetId;
    std::string itsTableName;
    std::string itsRecCntDsetIni;

    bool itsUseLevelValue;
    bool itsUseInverseLevelValue;

    bool itsDryRun;

    // TODO: initializing this struct

    struct fc_info 
    {
      fc_info() {};

      long grib_size;
      long ednum;
      long centre;
      long discipline;
      long category;
      long process;
      long year;
      long month;
      long day;
      long hour;
      long minute;
      long stepType;
      long startstep;
      long endstep;
      long gridtype;
      long timeUnit;
      long ni;
      long nj;
      double lat;
      double lon;
      double di;
      double dj;
      long step;
      long param;
      long novers;
      long levtype;
      long level1;
      long level2;
      long lvl1_lvl2;
      long locdef;
      long ldeftype;
      long ldefnumber;
      long fcst_per;
      std::string base_date;
      std::string parname;
      std::string levname;
      std::string grtyp;
      std::string eps_specifier;
      std::string filetype;
      std::string filename;
      std::string hostname;
    };

    bool WriteAS(const fc_info &info);

    std::string REFFileName(const fc_info &info);

  private:
    bool ReadREFEnvironment();
    void Init();

    char *base;
    char *host;


};

#endif
