#ifndef __BDAPLOADER__
#define __BDAPLOADER__

#include <string>
#include "fc_info.h"
#include <memory>
#include "NFmiNeonsDB.h"
#include "NFmiRadonDB.h"

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

	NFmiNeonsDB& NeonsDB() const;
	NFmiRadonDB& RadonDB() const;
	
private:
    void InitPool(const std::string& username, const std::string& password, const std::string& database);

    std::string itsUsername;
    std::string itsPassword;
    std::string itsDatabase;

    std::string itsGeomName;
    std::string itsModelType;
    std::string itsDsetId;
    std::string itsTableName;

    bool ReadREFEnvironment();
    void Init();

    char *base;
    std::string itsHostname;

    std::unique_ptr<NFmiNeonsDB> itsNeonsDB;
    std::unique_ptr<NFmiRadonDB> itsRadonDB;

};

#endif
