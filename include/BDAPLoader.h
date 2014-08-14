#ifndef __BDAPLOADER__
#define __BDAPLOADER__

/*
 * class BDAPLoader
 *
 */

#include <string>
#include "fc_info.h"

class BDAPLoader 
{

  public:

    BDAPLoader();
    ~BDAPLoader();

    BDAPLoader(const BDAPLoader& other) = delete;
    BDAPLoader& operator=(const BDAPLoader& other) = delete;

    std::string REFFileName(const fc_info &info);

    bool WriteAS(const fc_info &info);
#ifdef NEON2
    bool WriteToNeon2(const fc_info &info);
#endif

private:

    std::string itsUsername;
    std::string itsPassword;
    std::string itsDatabase;

    std::string itsGeomName;
    std::string itsModelName;
    std::string itsModelType;
    std::string itsTypeSmt;
    std::string itsDsetId;
    std::string itsTableName;
    std::string itsRecCntDsetIni;

    bool ReadREFEnvironment();
    void Init();

    char *base;
    std::string itsHostname;


};

#endif
