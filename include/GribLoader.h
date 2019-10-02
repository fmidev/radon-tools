#pragma once

#include "BDAPLoader.h"
#include "NFmiGrib.h"
#include "options.h"
#include <atomic>
#include <boost/thread.hpp>
#include <mutex>
#include <string>

class GribLoader
{
   public:
	GribLoader();
	virtual ~GribLoader();

	bool Load(const std::string& theInfile);

   protected:
	void Run(short threadId);
	bool DistributeMessages(NFmiGribMessage& newMessage, unsigned int& messageNo);
	void Process(BDAPLoader& databaseLoader, NFmiGribMessage& message, short threadId, unsigned int messageNo);
	virtual std::string GetFileName(BDAPLoader& databaseLoader, const fc_info& g);
	bool IsGrib(const std::string& theFileName);

	NFmiGrib itsReader;

	std::vector<std::string> parameters;
	std::vector<std::string> levels;

	std::atomic<int> g_success;
	std::atomic<int> g_skipped;
	std::atomic<int> g_failed;

	std::mutex distMutex, tableMutex, ssMutex;

	std::set<std::string> analyzeTables;
	std::set<std::string> ssStateInformation;
	std::string inputFileName;
};
