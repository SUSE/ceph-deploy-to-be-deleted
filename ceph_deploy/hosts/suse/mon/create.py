from ceph_deploy.hosts import common
from ceph_deploy.lib import remoto
from ceph_deploy.util import systemd
import logging

LOG = logging.getLogger(__name__)

def create(distro, args, monitor_keyring):
    hostname = distro.conn.remote_module.shortname()
    common.mon_create(distro, args, monitor_keyring, hostname)
    if distro.init == 'systemd':
        prefix = ""
        if args.cluster != "ceph":
            prefix = "%s-" % (args.cluster)
            systemd.build_up(distro, args)
        remoto.process.run(
            distro.conn,
            [
                'systemctl',
                'enable',
                '{prefix}ceph-mon@{hostname}'.format(hostname=hostname,
                    prefix=prefix)
            ],
            timeout=7,
        )
        remoto.process.run(
            distro.conn,
            [
                'systemctl',
                'start',
                '{prefix}ceph-mon@{hostname}'.format(hostname=hostname,
                    prefix=prefix)
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
    elif distro.init == 'sysvinit':
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
