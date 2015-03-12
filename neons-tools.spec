%define dist .el6

%if %{defined suse_version}
%define dist .sles11
%endif

%define PACKAGENAME neons-tools
Name:           %{PACKAGENAME}
Version:        15.3.12
Release:        1%{?dist}.fmi
Summary:        Tools for neons environment
Group:          Applications/System
License:        FMI
URL:            http://www.fmi.fi
Source0: 	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  libfmigrib-devel >= 15.3.10
BuildRequires:  libfmidb-devel >= 15.3.10
BuildRequires:  grib_api-devel >= 1.13.0
BuildRequires:  boost-devel >= 1.54
Requires:       hdf5
Requires:       oracle-instantclient-basic
%if %{defined suse_version}
Requires:	libjasper
Requires:	libnetcdf4 >= 4.0.1
%else
Requires:       jasper-libs
Requires:       netcdf >= 4.1.1
Requires:	python-psycopg2
Requires:	python-bunch
Provides:	radon_tables.py
Provides:	previ_to_radon.py
%endif
Provides:	grid_to_neons
Provides:	create_grid_tables

%description
Neons-tools includes programs for loading data to neons DB.
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
%{_bindir}/create_grid_tables

%if "%{dist}" == ".el6"
%{_bindir}/radon_tables.py
%{_bindir}/previ_to_radon.py
%endif

%changelog
* Thu Mar 12 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.12-1.fmi
- Fix issue with netcdf file loading
* Wed Mar 11 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.11-1.fmi
- Reducing default thread count to two
* Tue Mar 10 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.3.10-1.fmi
- New version of grid_to_neons with threading support
* Wed Feb 18 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.2.18-1.fmi
- Fix crash when radon producer information was missing
- Link with grib_api 1.13.0
* Wed Jan 26 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.1.26-1.fmi
- Changes in fmidb
* Wed Jan  7 2015 Mikko Partio <mikko.partio@fmi.fi> - 15.1.7-1.fmi
- Changes in fmidb
* Tue Dec  2 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.12.2-1.fmi
- Minor changes
* Tue Nov  5 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.11.5-1.fmi
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
* Thu Sep 24 2014 Mikko Partio <mikko.partio@fmi.fi> - 14.9.24-1.fmi
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
