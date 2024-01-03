%if !0%{?version:1}
%define version 23.10.17
%endif

%if !0%{?release:1}
%define release 1
%endif

%define distnum %(/usr/lib/rpm/redhat/dist.sh --distnum)

%if %{distnum} == 8
%define boost boost169
%else
%define boost boost
%endif

Name:           radon-tools
Version:        %{version}
Release:        %{release}%{dist}.fmi
Summary:        Tools for radon environment
Group:          Applications/System
License:        FMI
URL:            http://www.fmi.fi
Source0: 	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  libfmigrib-devel >= 23.12.8
BuildRequires:  libfmidb-devel >= 23.7.27
BuildRequires:  libfminc-devel >= 23.12.27
BuildRequires:  eccodes-devel
BuildRequires:  libs3-devel >= 4.1
BuildRequires:  himan-lib >= 23.12.28
BuildRequires:  himan-lib-devel >= 23.12.28
BuildRequires:  himan-plugins-devel >= 23.9.25
BuildRequires:  fmt-devel >= 7.1.0
BuildRequires:  python3-scons
BuildRequires:	gdal35-devel
BuildRequires:  %{boost}-devel
BuildRequires:  make
BuildRequires:  gcc-c++
BuildRequires:  python3-distro
BuildRequires:  unixODBC-devel
Requires:       hdf5
Requires:	libfmigrib >= 23.12.8
Requires:	libfmidb >= 23.10.16
Requires:	libfminc >= 23.12.27
Requires:	himan-lib >= 23.12.28
Requires:	himan-plugins >= 23.9.25
Requires:	netcdf-cxx
Requires:	eccodes
Requires:	libs3 >= 4.1
Requires:       python3-pytz
Requires:       python3-dateutil
Requires:       python3-boto3
Requires:	gdal35-libs
Requires:	libpqxx >= 7.7.0
Requires:       python3-psycopg2
Requires:	python3-dotenv
Requires:       jasper-libs
Requires:       netcdf >= 4.1.1
Requires:       %{boost}-program-options
Requires:       %{boost}-iostreams
Requires:	unixODBC
Provides:	radon_tables.py
Provides:	previ_to_radon.py
Provides:	geom_to_radon.py
Provides:	grid_to_radon
Obsoletes:	neons-tools

AutoReqProv: no

%description
radon-tools includes programs for loading data to radon DB.
Table creation tools are also included.

%prep
%setup -q -n "radon-tools"

%build
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install bindir=$RPM_BUILD_ROOT/%{_bindir}
ln -s %{_bindir}/grid_to_radon %{buildroot}/%{_bindir}/grid_to_neons

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%{_bindir}/grid_to_radon
%{_bindir}/grid_to_neons
%{_bindir}/radon_tables.py
%{_bindir}/previ_to_radon.py
%{_bindir}/geom_to_radon.py
%{_bindir}/calc_hybrid_level_height.py

%changelog
* Tue Oct 17 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.10.17-1.fmi
- New fmidb
* Wed Oct  4 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.10.4-1.fmi
- Support general level with calc_hybrid_level_height.py
* Mon Sep 25 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.9.25-1.fmi
- Updated himan-plugins
* Mon Sep 18 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.9.18-1.fmi
- Add option --wait-timeout to grid_to_radon
* Thu Sep 14 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.9.14-1.fmi
- Allow explicit setting of sslmode
* Wed Sep 13 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.9.13-1.fmi
- Detect file type from s3 based files
* Tue Aug  1 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.8.1-1.fmi
- New fmidb
* Mon Jul 24 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.7.24-1.fmi
- gdal35
* Thu Mar  9 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.3.9-1.fmi
- Newer himan libraries
* Thu Feb 23 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.2.23-1.fmi
- Newer himan libraries
* Thu Feb  2 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.2.2-1.fmi
- Remove some boost dependencies
* Fri Jan 27 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.1.27-1.fmi
- Fix memory leak when reading gribs
* Fri Jan 20 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.1.20-1.fmi
- Fix command line option --smartmet-server-table-name
* Mon Jan 16 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.1.16-1.fmi
- Updated himan-plugins
* Wed Jan  4 2023 Mikko Partio <mikko.partio@fmi.fi> - 23.1.4-1.fmi
- Clean table ss_forecast_status
* Mon Dec 19 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.12.19-1.fmi
- Fix s3 reading if hostname has protocol
* Mon Nov 28 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.11.28-1.fmi
- Minor change to geotiff loading
* Fri Nov 25 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.11.25-1.fmi
- Bugfix for radon_tables.py
* Wed Nov  9 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.11.9-1.fmi
- SQL query optimization
* Wed Aug 24 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.8.24-1.fmi
- pqxx 7.7
* Wed Jun  1 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.6.1-1.fmi
- Update to previ_to_radon.py
* Tue Feb  1 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.2.1-1.fmi
- gdal34
* Mon Jan 17 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.1.17-1.fmi
- Updated himan-libs
* Fri Jan 14 2022 Mikko Partio <mikko.partio@fmi.fi> - 22.1.14-1.fmi
- New gdal and pqxx
* Wed Dec  8 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.12.8-1.fmi
- Support loading geotiff data from s3
* Tue Sep 28 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.9.28-1.fmi
- Fix crash when loading too old data
* Thu Sep 16 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.9.16-1.fmi
- Bugfix when reading stereographic grid information
* Wed Sep  8 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.9.8-1.fmi
- Update ss_state with information about loaded netcdf fields
* Wed Aug 11 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.8.11-1.fmi
- New himan lib & plugins
* Mon Aug  2 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.8.2-1.fmi
- New libpqxx & himan
* Thu Jun 10 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.6.10-1.fmi
- New himan-plugins
* Mon May 17 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.5.17-1.fmi
- Correct forecast type value for ss_state
* Tue May 11 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.5.11-1.fmi
- New himan-lib
* Thu May  6 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.5.6-1.fmi
- Fix for previ_to_radon.py
* Wed May  5 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.5.5-1.fmi
- Add option to write loaded metadata to file
* Fri Apr 30 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.4.30-1.fmi
- Change remainin python scripts to py3
* Mon Apr 26 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.4.26-1.fmi
- Renaming from neons-tools to radon-tools
* Mon Apr 12 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.4.12-1.fmi
- New himan-lib
* Tue Apr  6 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.4.6-1.fmi
- New himan-lib
* Mon Mar 22 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.3.22-1.fmi
- New himan-lib
* Tue Mar  2 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.3.2-1.fmi
- New himan-lib
* Tue Feb 23 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.2.23-1.fmi
- New himan-lib
* Fri Feb 12 2021 Mikko Partio <mikko.partio@fmi.fi> - 21.2.12-1.fmi
- Add variable to specify S3 protocol
* Thu Dec  3 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.12.3-1.fmi
- Disallow in-place load to multiple tables
* Wed Dec  2 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.12.2-1.fmi
- New fmigrib
* Mon Nov 23 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.11.23-1.fmi
- Minor logging changes
- New himan-lib
* Wed Nov 18 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.11.18-1.fmi
- fmt7
* Mon Oct 26 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.26-1.fmi
- himan header changes
* Mon Oct 19 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.19-1.fmi
- Add dependency to fmt (a string formatting library)
* Thu Oct 15 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.15-1.fmi
- Control masala_base directory environment variable name
* Wed Oct 14 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.14-1.fmi
- Fix for grib loading
* Thu Oct  8 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.8-1.fmi
- New himan libraries
* Mon Oct  5 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.10.5-1.fmi
- Changes in netcdf loading
* Mon Sep 28 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.9.28-1.fmi
- New fminc
* Thu Sep  3 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.9.3-2.fmi
- Compile with libs3 4.1
* Thu Sep  3 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.9.3-1.fmi
- Logging changes for radon_tables.py
* Mon Aug 31 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.31-1.fmi
- Use boto3 for removing files from s3
* Thu Aug 27 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.27-2.fmi
- Fix for forecast_type_value @ss_state
* Thu Aug 27 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.27-1.fmi
- Support stereographic projection with netcdf
* Tue Aug 25 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.25-1.fmi
- Fix for netcdf/grib file extensions
* Mon Aug 24 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.24-1.fmi
- Fix for handling command line options for file type
* Wed Aug 19 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.19-1.fmi
- Change in himan::util::Split()
* Tue Aug 18 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.18-1.fmi
- Fix for handling command line argument -L
* Mon Aug 17 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.17-1.fmi
- Do not extract region from S3 hostname for non-AWS hosts
* Wed Aug 12 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.12-1.fmi
- Fix geometry query for rotated areas
* Mon Aug  3 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.8.3-1.fmi
- Major refactoring using himan components
* Wed Jul 29 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.7.29-1.fmi
- Add support for removing files from s3
* Wed Jul  8 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.7.8-1.fmi
- New fmidb
* Wed Apr 29 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.29-2.fmi
- Proper time mask handling for geotiff
* Wed Apr 29 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.29-1.fmi
- Fix update of as_grid for netcdf/geotiff
* Mon Apr 27 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.27-1.fmi
- Fix bug that caused early exit
* Fri Apr 24 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.24-1.fmi
- Initial support of geotiff
* Mon Apr 20 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.20-1.fmi
- boost 1.69
* Mon Apr  6 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.4.6-1.fmi
- fmidb ABI change
* Thu Mar 26 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.3.28-1.fmi
- Add option to skip directory structure check
* Wed Mar 18 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.3.18-1.fmi
- New release
* Tue Mar 17 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.3.17-1.fmi
- New release
* Sat Mar 14 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.3.14-1.fmi
- More fixes to S3 loading
* Tue Mar  3 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.3.2-1.fmi
- More fixes to S3 loading
* Wed Feb 19 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.2.19-1.fmi
- Remove files instead of directories
* Mon Jan 27 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.26-1.fmi
- More fixes to S3 loading
* Fri Jan 24 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.24-1.fmi
- More fixes to S3 loading
* Wed Jan 22 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.22-1.fmi
- More fixes to S3 loading
* Thu Jan 16 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.16-1.fmi
- Fix to loading multiple files from S3
* Tue Jan  7 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.7-1.fmi
- Fix for as_grid update with S3 based files
* Thu Jan  2 2020 Mikko Partio <mikko.partio@fmi.fi> - 20.1.2-1.fmi
- Add security context for S3 read
- Fix for radon_tables monthly partitioning
* Tue Dec 17 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.12.17-2.fmi
- grid_to_neons: read port from environment
* Tue Dec 17 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.12.17-1.fmi
- Fix bug in previ table drop
* Mon Dec 16 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.12.16-1.fmi
- Logging output changes to radon_tables.py
* Tue Dec  3 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.12.3-2.fmi
- Allow analysis time where minute != 0
* Tue Dec  3 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.12.3-1.fmi
- STU-11538: Big fix to partitioning type <> analysistime
* Tue Nov 12 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.11.12-1.fmi
- Copy foreign keys to data tables on creation
* Mon Nov 11 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.11.11-1.fmi
- Bugfix
* Thu Nov  7 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.11.17-1.fmi
- Enable indexing of files in S3 storage
* Thu Oct 17 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.17-1.fmi
- Enable byte_offset&byte_length columns
* Tue Oct 15 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.15-1.fmi
- Bugfixes
* Mon Oct 14 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.14-1.fmi
- radon_tables.py to python3
- Remove previ_to_neons.py
- Tweaking directory removal strategy due to in-place insert functionality
- Misc bug fixes
* Fri Oct 11 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.11-1.fmi
- Minor bugfix
* Thu Oct 10 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.10-1.fmi
- in place insert for grid_to_radon
* Mon Oct  7 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.10.7-1.fmi
- fmigrib ABI change
* Wed Apr  3 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.4.3-1.fmi
- Bugfix for Netcdf loading
* Thu Jan 17 2019 Mikko Partio <mikko.partio@fmi.fi> - 19.1.17-1.fmi
- Change some GRIB2 level definitions to suite radon
* Mon Oct 15 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.15-1.fmi
- ss_state table changes
* Tue Oct  9 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.9-1.fmi
- Fix netcdf parameter id handling
* Mon Oct  8 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.8-2.fmi
- Code cleanup
- grib file path changed to have producer id instead of centre_ident
* Mon Oct  8 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.8-1.fmi
- Use typeOfStatisticalProcessing to determine param_id in GRIB2
* Thu Oct  4 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.4-2.fmi
- Remove switch -u
* Thu Oct  4 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.4-1.fmi
- Remove empty directories
* Tue Oct  2 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.10.2-1.fmi
- Retry trigger creation if deadlock occurs
* Wed Sep 19 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.9.19-1.fmi
- Ignore errors at file delete
* Wed Sep 12 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.9.12-1.fmi
- Remove rows at ss_state before data
* Thu Sep  6 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.9.6-1.fmi
- Another change to radon_tables.py
* Wed Sep  5 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.9.5-1.fmi
- Change to radon_tables.py
* Wed Aug 22 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.8.22-1.fmi
- fminc changes
* Mon May 14 2018 Mikko Aalto <mikko.aalto@fmi.fi> - 18.5.14-1.fmi
- Another Fix to netcdf loading
* Tue May  8 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.5.8-1.fmi
- Fix to netcdf loading issue
* Thu May  3 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.5.3-1.fmi
- lcc projection with netcdf
- Read from stdin
* Wed May  2 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.5.2-1.fmi
- as_grid changes
* Tue Apr 10 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.4.10-1.fmi
- New boost
* Wed Mar  7 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.3.7-1.fmi
- Geometry name to file path
* Tue Feb 20 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.2.20-1.fmi
- fmigrib api change
* Mon Feb 19 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.2.19-1.fmi
- Fix grib2 analysis loading
- Add minutes to originTime
* Wed Jan 24 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.1.24-1.fmi
- fmigrib api change
* Mon Jan 22 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.1.22-1.fmi
- Bugfix for netcdf time read
* Mon Dec 11 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.12.11-1.fmi
- New grib level type
* Mon Nov  6 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.11.6-1.fmi
- Add ss_state table update
* Fri Oct 27 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.10.27-1.fmi
- Code cleanup
* Wed Oct 25 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.10.25-1.fmi
- New fmigrib
* Thu Oct 19 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.10.19-1.fmi
- Remove oracle support from grid_to_neons
* Tue Oct 10 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.10.10-1.fmi
- Support multiple input files at command line
* Mon Sep 18 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.9.18-1.fmi
- Use grib1 parameter cache warming
* Fri Sep 15 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.9.15-1.fmi
- Use fmidb function to get target table information
* Tue Aug 29 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.8.29-1.fmi
- New boost
* Thu Jun 22 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.6.22-1.fmi
- Run manual ANALYZE after insert also with NetCDF and previ data
* Thu Jun  8 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.6.8-1.fmi
- previ_to_radon.py discards CSV headers
* Mon May 29 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.5.29-1.fmi
- Run manual ANALYZE after first row insertion
* Mon May 15 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.5.15-1.fmi
- New CSV format and previ database table layout
* Thu Apr  6 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.4.6-1.fmi
- Add geom_to_radon.py
- New fmigrib
- New fmidb
* Thu Mar  9 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.3.9-1.fmi
- Fix SQL query for radon update with ensemble members
* Mon Feb 27 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.2.27-1.fmi
- New fminc
* Wed Feb 22 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.2.22-1.fmi
- Explicit locking for radon_tables.py
* Tue Jan 17 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.1.17-1.fmi
- New time mask for netcdf data
* Wed Jan  4 2017 Mikko Partio <mikko.partio@fmi.fi> - 17.1.4-1.fmi
- New time mask for netcdf data
- General code cleanup
* Fri Dec  9 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.12.9-1.fmi
- Changes in fmidb
* Thu Nov 17 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.11.17-1.fmi
- Changes to index loading
* Tue Nov  8 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.11.8-1.fmi
- Changes in fmidb
* Wed Oct 26 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.10.26-1.fmi
- Support level_value2 column in radon database
* Tue Oct 25 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.10.25-1.fmi
- SLES compatibility 
* Thu Oct 20 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.10.20-2.fmi
- Add full path of grib file to index header
* Thu Oct 20 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.10.20-1.fmi
- Replacing grib_api with eccodes
- Some fixes to index loading
* Thu Sep 29 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.29-1.fmi
- Lambert support
* Thu Sep 15 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.15-1.fmi
- GribIndexLoader functionality added
* Thu Sep  8 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.8-3.fmi
- API change for fmigrib and fmidb
* Thu Sep  8 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.8-2.fmi
- radon_tables.py bugfix
* Thu Sep  8 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.8-1.fmi
- New release
* Wed Sep  7 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.9.7-1.fmi
- New release
* Wed Aug 24 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.8.24-1.fmi
- New release
* Tue Aug 23 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.8.23-2.fmi
- Fixing typo with radon_tables.py
* Tue Aug 23 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.8.23-1.fmi
- New release
* Mon Aug 15 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.8.15-1.fmi
- New fmigrib
* Wed Jun 15 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.6.15-1.fmi
- Fix for radon load
* Mon Jun 13 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.6.13-1.fmi
- New release
* Thu Jun  2 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.6.2-1.fmi
- New release
* Wed Jun  1 2016 Mikko Aalto <mikko.aalto@fmi.fi> - 16.6.1-1.fmi
- New grib_api 1.15
* Thu May 26 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.5.26-1.fmi
- Changes in fmidb/NFmiRadonDB
* Thu Apr 28 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.4.28-1.fmi
- Support new radon producer class_id = 4 
* Fri Feb 12 2016 Mikko Partio <mikko.partio@fmi.fi> - 16.2.12-1.fmi
- New fmidb
* Tue Dec 15 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.12.15-1.fmi
- Support for copernicus data
* Thu Dec  3 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.12.3-1.fmi
- New fminc
* Wed Nov 18 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.11.18-1.fmi
- New release
* Fri Oct  9 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.10.9-1.fmi
- Minor fixes
* Tue Oct  6 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.10.6-1.fmi
- Bugfix for radon grib loading
* Wed Sep 30 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.9.30-1.fmi
- Change in fminc
* Tue Sep 22 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.9.22-1.fmi
- previ_to_radon.py fix for mos loading
* Tue Sep 15 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.9.15-1.fmi
- New fminc
* Wed Sep  2 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.9.2-1.fmi
- fmidb api change
- grib_api 1.14
* Mon Aug 24 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.8.24-1.fmi
- fmigrib api change
* Mon Aug 10 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.8.10-2.fmi
- Fix for previ_to_radon.py
* Mon Aug 10 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.8.10-1.fmi
- Fix for radon_tables.py
* Wed Jun 24 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.6.24-1.fmi
- Changed NFmiRadonDB to use NFmiPostgreSQL
* Mon May 11 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.5.11-1.fmi
- Update as_previ record count
* Fri May  8 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.5.8-1.fmi
- Less hardcoding and assumptions in previ_to_neons.py 
* Fri Apr 24 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.4.24-1.fmi
- Fixing radon support for grid_to_neons when only radon is used
* Thu Apr 16 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.4.16-2.fmi
- Adding previ_to_neons.py
* Thu Apr 16 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.4.16-1.fmi
- Changes in fmigrib
* Tue Apr  7 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.4.7-1.fmi
- Improved eps support
* Mon Mar 30 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.30-1.fmi
- Thread safety issues
* Wed Mar 18 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.18-1.fmi
- More detailed timing information
* Tue Mar 17 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.17-1.fmi
- Optimizations in grib loading
* Mon Mar 16 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.16-1.fmi
- Update to radon trigger
* Thu Mar 12 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.12-1.fmi
- Fix issue with netcdf file loading
* Wed Mar 11 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.11-1.fmi
- Reducing default thread count to two
* Tue Mar 10 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.10-1.fmi
- New version of grid_to_neons with threading support
* Wed Feb 18 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.2.18-1.fmi
- Fix crash when radon producer information was missing
- Link with grib_api 1.13.0
* Mon Jan 26 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.1.26-1.fmi
- Changes in fmidb
* Wed Jan  7 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.1.7-1.fmi
- Changes in fmidb
* Tue Dec  2 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.12.2-1.fmi
- Minor changes
* Wed Nov  5 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.11.5-1.fmi
- Minor changes from yesterday
* Tue Nov  4 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.11.4-1.fmi
- Fix in radon_tables.py
* Thu Oct 30 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.10.30-1.fmi
- Use level type to fetch param name
* Wed Oct 22 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.10.22-1.fmi
- More renaming
* Mon Oct 20 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.10.20-1.fmi
- Renaming neon2 --> radon
* Wed Oct  8 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.10.8-1.fmi
- Changes in fmigrib
* Wed Sep 24 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.24-1.fmi
- Small fixes in previ_to_neon2.py and neon2_tables.py
* Thu Sep 18 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.18-1.fmi
- previ_to_neon2.py --bulk
* Mon Sep 15 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.15-1.fmi
- Small fixes
* Mon Sep  8 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.8-1.fmi
- Bugfix in previ_to_neon2.py
* Fri Sep  5 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.5-1.fmi
- Bugfixing in previ table creation
* Thu Sep  4 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.4-1.fmi
- Additional features to previ_to_neon2.py and neon2_tables.py
* Tue Sep  2 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.2-1.fmi
- Adding features to previ_to_neon2.py
- Fixes for grid_to_neons netcdf
* Mon Sep  1 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.1-1.fmi
- Adding features to previ_to_neon2.py
* Fri Aug 29 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.29-1.fmi
- Adding previ_to_neon2.py
* Fri Aug 22 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.22-1.fmi
- Changes to neon2 loading
* Thu Aug 21 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.21-1.fmi
- New release to link with new version of fminc
- Changes in neon2_tables.py
* Tue Aug 19 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.19-4.fmi
- Bugfixes in grid_to_neons
* Tue Aug 19 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.19-3.fmi
- More Changes in FmiDB
* Tue Aug 19 2014 Andreas Tack <andreas.tack@fmi.fi> - 14.8.19-2.fmi
- Changes in FmiDB
* Tue Aug 19 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.19-1.fmi
- Renaming create_grid_tables.py to neon2_tables.py
- Minor fixes
* Tue Aug  5 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.12-1.fmi
- Adding create_grid_tables.py
* Tue Aug  5 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.8.5-1.fmi
- Supporting UKMO model
* Mon Apr  7 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.4.7-1.fmi
- Add forecast time-based subdirectory to refstorage file path
* Fri Jan 31 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.1.31-1.fmi
- Remove hard coded Harmonie-fix related to time range indicator and code table 1
- Improve logging 
* Thu Jan  2 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.1.2-1.fmi
- Link with grib_api 1.11.0
* Fri Nov 29 2013 Mikko Partio <mikko.partio@fmi.fi> - 13.11.29-1.fmi
- Do not count grib messages 
- Support for new Harmonie
* Tue Nov 19 2013 Mikko Partio <mikko.partio@fmi.fi> - 13.11.19-1.fmi
- API breaking changes in fmidb
* Fri Nov 15 2013 Mikko Partio <mikko.partio@fmi.fi> - 13.11.15-1.fmi
- Fix for OPER-382
* Wed Oct  9 2013 Mikko Partio <mikko.partio@fmi.fi> - 13.10.9-1.fmi
- Use wetodb database user
* Tue Oct  8 2013 Mikko Aalto <mikko.aalto@fmi.fi> - 13.10.8-1.fmi
- Initial build
