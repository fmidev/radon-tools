FROM rockylinux/rockylinux:8

# Add smartmet open and epel repositories
RUN rpm -ivh https://download.fmi.fi/smartmet-open/rhel/8/x86_64/smartmet-open-release-latest-8.noarch.rpm \
             https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

# Use eccodes from smartmet open
RUN dnf -y install dnf-plugins-core && \
    dnf config-manager --set-enabled powertools && \
    dnf -y module disable postgresql && \
    dnf config-manager --setopt="epel.exclude=eccodes*" --save && \
    dnf -y install s3cmd radon-tools && \
    dnf -y clean all
