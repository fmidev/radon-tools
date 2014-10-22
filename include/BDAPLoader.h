#ifndef __BDAPLOADER__
#define __BDAPLOADER__

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
    bool WriteToRadon(const fc_info &info);

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

    bool itsUseRadon;

};

#endif
