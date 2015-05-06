from ceph_deploy.hosts import common
from ceph_deploy.lib import remoto
from ceph_deploy.util.system import systemd_defaults_clustername
import logging

LOG = logging.getLogger(__name__)


def create(distro, args, monitor_keyring):
    hostname = distro.conn.remote_module.shortname()
    common.mon_create(distro, args, monitor_keyring, hostname)
    systemd_defaults_clustername(distro.conn, args.cluster)
    if distro.init == 'systemd':  # Ubuntu uses upstart
        remoto.process.run(
            distro.conn,
            [
                'systemctl',
                'enable',
                'ceph-mon@{hostname}'.format(hostname=hostname)
            ],
            timeout=7,
        )
        remoto.process.run(
            distro.conn,
            [
                'systemctl',
                'start',
                'ceph-mon@{hostname}'.format(hostname=hostname)
            ],
            timeout=7,
        )
        remoto.process.run(
            distro.conn,
            [
                'systemctl',
                'enable',
                'ceph.target'
            ],
            timeout=7,
        )
    elif distro.init == 'sysvinit':  # Debian uses sysvinit
        remoto.process.run(
            distro.conn,
            [
                '/etc/init.d/ceph',
                '-c',
                '/etc/ceph/{cluster}.conf'.format(cluster=args.cluster),
                'start',
                'mon.{hostname}'.format(hostname=hostname)
            ],
            timeout=7,
        )
    else:
        raise RuntimeError('create cannot use init %s' % distro.init)
