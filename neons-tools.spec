%define dist .el6

%if %{defined suse_version}
%define dist .sles11
%endif

%define PACKAGENAME neons-tools
Name:           %{PACKAGENAME}
Version:        14.10.8
Release:        1%{?dist}.fmi
Summary:        Tools for neons environment
Group:          Applications/System
License:        LGPLv3
URL:            http://www.fmi.fi
Source0: 	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  libfmigrib-devel
BuildRequires:  libfmidb-devel >= 14.8.19
BuildRequires:  grib_api-devel >= 1.11.0
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
Provides:	neon2_tables.py
Provides:	previ_to_neon2.py
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
make %{?_smp_mflags} CC=/usr/bin/g++-4.6 INCLUDE=-I/lustre/apps/partio/auxlibs
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
%{_bindir}/neon2_tables.py
%{_bindir}/previ_to_neon2.py
%endif

%changelog
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
