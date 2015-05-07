from ceph_deploy.util import templates, pkg_managers
from ceph_deploy.lib import remoto
import logging
LOG = logging.getLogger(__name__)


def install(distro, version_kind, version, adjust_repos):
    release = distro.release
    machine = distro.machine_type

    if version_kind in ['stable', 'testing']:
        key = 'release'
    else:
        key = 'autobuild'


    distro_name = None
    if distro.codename == 'Mantis':
        distro_name = 'openSUSE_12.2'

    LOG.warning('distro.codename=%s' % (distro.codename))
    if (distro.name == "SUSE Linux Enterprise Server"):
        if (str(distro.release) == "11"):
           distro_name = 'SLE_11_SP3'
        if (str(distro.release) == "12"):
           distro_name = 'SLE_12'

    if distro_name == None:
        LOG.warning('Untested version of %s: assuming compatible with SUSE Linux Enterprise Server 11', distro.name)
        distro_name = 'SLE_12'

    LOG.warning('distro_name=%s' % (distro_name))

    remoto.process.run(
        distro.conn,
        [
            'zypper',
            '--non-interactive',
            'refresh'
            ],
        )

    remoto.process.run(
        distro.conn,
        [
            'zypper',
            '--non-interactive',
            '--quiet',
            'install',
            'ceph'
            ],
        )
