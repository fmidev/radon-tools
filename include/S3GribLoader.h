#pragma once

#include <string>

class S3GribLoader
{
   public:
	S3GribLoader();
	~S3GribLoader() = default;

	bool Load(const std::string& theInfile) const;

   private:
	void Initialize();
	void ReadFileStream(const std::string& theFileName, size_t startByte, size_t byteCount) const;

	char* itsHost;
	char* itsAccessKey;
	char* itsSecretKey;
	char* itsSecurityToken;
};
