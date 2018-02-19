%define distnum %(/usr/lib/rpm/redhat/dist.sh --distnum)

%if %{defined suse_version}
%define dist .sles11
%endif

%define PACKAGENAME neons-tools
Name:           %{PACKAGENAME}
Version:        18.2.19
Release:        1.el7.fmi
Summary:        Tools for neons environment
Group:          Applications/System
License:        FMI
URL:            http://www.fmi.fi
Source0: 	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  libfmigrib-devel >= 18.1.24
BuildRequires:  libfmidb-devel >= 17.9.18
BuildRequires:  libfminc-devel >= 17.2.27
BuildRequires:  eccodes-devel
BuildRequires:  scons
Requires:       hdf5
Requires:       oracle-instantclient-basic
Requires:	libfmigrib >= 18.1.24
Requires:	libfmidb >= 17.9.18
Requires:	libfminc >= 17.2.27
Requires:	netcdf-cxx
Requires:	python-dateutil
Requires:	libpqxx
Requires:	eccodes

%if %{defined suse_version}
BuildRequires:  boost-devel >= 1.53
Requires:	libjasper
Requires:	libnetcdf4 >= 4.0.1
%else
BuildRequires:  boost-devel >= 1.65
Requires:       jasper-libs
Requires:       netcdf >= 4.1.1
Requires:	python-psycopg2
Requires:	python-bunch
Requires:       pytz
Requires:       libpqxx
Provides:	radon_tables.py
Provides:	previ_to_radon.py
Provides:       previ_to_neons.py
Provides:	geom_to_radon.py
%endif

Provides:	grid_to_radon

AutoReqProv: no

%description
Neons-tools includes programs for loading data to radon DB.
Table creation tools are also included.

%prep
%setup -q -n "%{PACKAGENAME}"

%build
%if %{defined suse_version}
CXX=/usr/bin/g++-4.6 INCLUDE=/lustre/apps/partio/auxlibs make %{?_smp_mflags}
%else
make %{?_smp_mflags}
%endif

%install
rm -rf $RPM_BUILD_ROOT
make install bindir=$RPM_BUILD_ROOT/%{_bindir}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%{_bindir}/grid_to_neons
%{_bindir}/grid_to_radon

%if %{undefined suse_version}
%{_bindir}/radon_tables.py
%{_bindir}/previ_to_radon.py
%{_bindir}/previ_to_neons.py
%{_bindir}/geom_to_radon.py
%endif

%changelog
* Mon Feb 19 2018 Mikko Partio <mikko.partio@fmi.fi> - 18.2.19-1.fmi
- Fix grib2 analysis loading
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
