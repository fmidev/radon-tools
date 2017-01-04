#ifndef GRIBCREATE_H
#define GRIBCREATE_H

#include <map>
#include <string>

class GribCreate
{
   public:
	GribCreate();
	~GribCreate();

	bool Create(const std::string &theDate, const std::string &tache);
	std::map<std::string, std::string> GetModelType(const std::string &theGroup);
	void DryRun(bool theDryRun);
	bool DryRun();

   private:
	std::string itsUsername;
	std::string itsPassword;
	std::string itsDatabase;

	bool itsDryRun;
};

#endif /* GRIBCREATE_H */
