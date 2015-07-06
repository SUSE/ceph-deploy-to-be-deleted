import mon  # noqa
from ceph_deploy.hosts.centos import pkg  # noqa
from ceph_deploy.hosts.centos.install import repo_install  # noqa
from install import install, mirror_install  # noqa
from uninstall import uninstall  # noqa

# Allow to set some information about this distro
#

distro = None
release = None
codename = None

def choose_init():
    """
    Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    return 'sysvinit'


def service_mapping(service):
    """
    Select the service name
    """
    service_mapping = { "apache" : "httpd",
        "ceph-rgw" : "ceph-rgw" }
    return service_mapping.get(service,service)
