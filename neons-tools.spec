%define PACKAGENAME neons-tools
Name:           %{PACKAGENAME}
Version:        13.11.15
Release:        1.fmi
Summary:        Tools for neons environment
Group:          Applications/System
License:        LGPLv3
URL:            http://www.fmi.fi
Source0: 	%{name}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  libfmigrib-devel
BuildRequires:  libfmidb
BuildRequires:  grib_api-devel
BuildRequires:  boost-devel >= 1.54
Requires:       jasper-libs
Requires:       netcdf >= 4.1.1
Requires:       hdf5
Requires:       oracle-instantclient-basic
Provides:	grid_to_neons
Provides:	create_grid_tables

%description
Neons-tools includes programs for loading data to neons DB.
Table creation tools are also included.

%prep
%setup -q -n "%{PACKAGENAME}"

%build
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install bindir=$RPM_BUILD_ROOT/%{_bindir}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%{_bindir}/grid_to_neons
%{_bindir}/create_grid_tables

%changelog
* Fri Nov 15 2013 Mikko Partio <mikko.partio@fmi.fi > 13.11.15-1.fmi
- Fix for OPER-382
* Wed Oct  9 2013 Mikko Partio <mikko.partio@fmi.fi > 13.10.9-1.fmi
- Use wetodb database user
* Tue Oct  8 2013 Mikko Aalto <mikko.aalto@fmi.fi> - 13.10.8-1.fmi
- Initial build
