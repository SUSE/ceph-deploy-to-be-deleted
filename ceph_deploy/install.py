import argparse
import logging
import os

from ceph_deploy import hosts
from ceph_deploy.cliutil import priority
from ceph_deploy.lib import remoto
from ceph_deploy.util import systemd

LOG = logging.getLogger(__name__)


def install(args):
    # XXX This whole dance is because --stable is getting deprecated
    if args.stable is not None:
        LOG.warning('the --stable flag is deprecated, use --release instead')
        args.release = args.stable
    if args.version_kind == 'stable':
        version = args.release
    else:
        version = getattr(args, args.version_kind)
    # XXX Tango ends here.

    version_str = args.version_kind

    if version:
        version_str += ' version {version}'.format(version=version)
    LOG.debug(
        'Installing %s on cluster %s hosts %s',
        version_str,
        args.cluster,
        ' '.join(args.host),
    )

    for hostname in args.host:
        LOG.debug('Detecting platform for host %s ...', hostname)
        distro = hosts.get(hostname, username=args.username)
        LOG.info(
            'Distro info: %s %s %s',
            distro.name,
            distro.release,
            distro.codename
        )

        if distro.init == 'sysvinit' and args.cluster != 'ceph':
            LOG.error('refusing to install on host: %s, with custom cluster name: %s' % (
                    hostname,
                    args.cluster,
                )
            )
            LOG.error('custom cluster names are not supported on sysvinit hosts')
            continue

        rlogger = logging.getLogger(hostname)
        rlogger.info('installing ceph on %s' % hostname)

        cd_conf = getattr(args, 'cd_conf', None)

        distro.install(
            distro,
            args.version_kind,
            version,
            args.adjust_repos
        )

        # Check the ceph version we just installed
        hosts.common.ceph_version(distro.conn)
        distro.conn.exit()


def should_use_custom_repo(args, cd_conf, repo_url):
    """
    A boolean to determine the logic needed to proceed with a custom repo
    installation instead of cramming everything nect to the logic operator.
    """
    if repo_url:
        # repo_url signals a CLI override, return False immediately
        return False
    if cd_conf:
        if cd_conf.has_repos:
            has_valid_release = args.release in cd_conf.get_repos()
            has_default_repo = cd_conf.get_default_repo()
            if has_valid_release or has_default_repo:
                return True
    return False


def custom_repo(distro, args, cd_conf, rlogger, install_ceph=None):
    """
    A custom repo install helper that will go through config checks to retrieve
    repos (and any extra repos defined) and install those

    ``cd_conf`` is the object built from argparse that holds the flags and
    information needed to determine what metadata from the configuration to be
    used.
    """
    default_repo = cd_conf.get_default_repo()
    if args.release in cd_conf.get_repos():
        LOG.info('will use repository from conf: %s' % args.release)
        default_repo = args.release
    elif default_repo:
        LOG.info('will use default repository: %s' % default_repo)

    # At this point we know there is a cd_conf and that it has custom
    # repos make sure we were able to detect and actual repo
    if not default_repo:
        LOG.warning('a ceph-deploy config was found with repos \
            but could not default to one')
    else:
        options = dict(cd_conf.items(default_repo))
        options['install_ceph'] = False if install_ceph is False else True
        extra_repos = cd_conf.get_list(default_repo, 'extra-repos')
        rlogger.info('adding custom repository file')
        try:
            distro.repo_install(
                distro,
                default_repo,
                options.pop('baseurl'),
                options.pop('gpgkey'),
                **options
            )
        except KeyError as err:
            raise RuntimeError('missing required key: %s in config section: %s' % (err, default_repo))

        for xrepo in extra_repos:
            rlogger.info('adding extra repo file: %s.repo' % xrepo)
            options = dict(cd_conf.items(xrepo))
            try:
                distro.repo_install(
                    distro,
                    xrepo,
                    options.pop('baseurl'),
                    options.pop('gpgkey'),
                    **options
                )
            except KeyError as err:
                raise RuntimeError('missing required key: %s in config section: %s' % (err, xrepo))


def install_repo(args):
    """
    For a user that only wants to install the repository only (and avoid
    installing ceph and its dependencies).
    """
    cd_conf = getattr(args, 'cd_conf', None)

    for hostname in args.host:
        LOG.debug('Detecting platform for host %s ...', hostname)
        distro = hosts.get(hostname, username=args.username)
        rlogger = logging.getLogger(hostname)

        LOG.info(
            'Distro info: %s %s %s',
            distro.name,
            distro.release,
            distro.codename
        )

        return custom_repo(distro, args, cd_conf, rlogger, install_ceph=False)


def uninstall(args):
    LOG.info('note that some dependencies *will not* be removed because they can cause issues with qemu-kvm')
    LOG.info('like: librbd1 and librados2')
    LOG.debug(
        'Uninstalling on cluster %s hosts %s',
        args.cluster,
        ' '.join(args.host),
        )

    for hostname in args.host:
        LOG.debug('Detecting platform for host %s ...', hostname)

        distro = hosts.get(hostname, username=args.username)
        LOG.info('Distro info: %s %s %s', distro.name, distro.release, distro.codename)
        rlogger = logging.getLogger(hostname)
        rlogger.info('uninstalling ceph on %s' % hostname)
        distro.uninstall(distro.conn)
        distro.conn.exit()


def purge(args):
    LOG.info('note that some dependencies *will not* be removed because they can cause issues with qemu-kvm')
    LOG.info('like: librbd1 and librados2')

    LOG.debug(
        'Purging from cluster %s hosts %s',
        args.cluster,
        ' '.join(args.host),
        )

    for hostname in args.host:
        LOG.debug('Detecting platform for host %s ...', hostname)

        distro = hosts.get(hostname, username=args.username)
        LOG.info('Distro info: %s %s %s', distro.name, distro.release, distro.codename)
        rlogger = logging.getLogger(hostname)
        rlogger.info('purging host ... %s' % hostname)
        distro.uninstall(distro.conn, purge=True)
        distro.conn.exit()


def purgedata(args):
    LOG.debug(
        'Purging data from cluster %s hosts %s',
        args.cluster,
        ' '.join(args.host),
        )

    installed_hosts = []
    for hostname in args.host:
        distro = hosts.get(hostname, username=args.username)
        ceph_is_installed = distro.conn.remote_module.which('ceph')
        if ceph_is_installed:
            installed_hosts.append(hostname)
        distro.conn.exit()

    if installed_hosts:
        LOG.error("ceph is still installed on: %s", installed_hosts)
        raise RuntimeError("refusing to purge data while ceph is still installed")

    for hostname in args.host:
        distro = hosts.get(hostname, username=args.username)
        LOG.info(
            'Distro info: %s %s %s',
            distro.name,
            distro.release,
            distro.codename
        )

        rlogger = logging.getLogger(hostname)
        rlogger.info('purging data on %s' % hostname)

        # Try to remove the contents of /var/lib/ceph first, don't worry
        # about errors here, we deal with them later on
        remoto.process.check(
            distro.conn,
            [
                'rm', '-rf', '--one-file-system', '--', '/var/lib/ceph',
            ]
        )

        # Tear down any custom systemd .service files.
        systemd.teardown(distro, args)

        # If we failed in the previous call, then we probably have OSDs
        # still mounted, so we unmount them here
        if distro.conn.remote_module.path_exists('/var/lib/ceph'):
            rlogger.warning(
                'OSDs may still be mounted, trying to unmount them'
            )
            remoto.process.run(
                distro.conn,
                [
                    'find', '/var/lib/ceph',
                    '-mindepth', '1',
                    '-maxdepth', '2',
                    '-type', 'd',
                    '-exec', 'umount', '{}', ';',
                ]
            )

            # And now we try again to remove the contents, since OSDs should be
            # unmounted, but this time we do check for errors
            remoto.process.run(
                distro.conn,
                [
                    'rm', '-rf', '--one-file-system', '--', '/var/lib/ceph',
                ]
            )

        remoto.process.run(
            distro.conn,
            [
                'rm', '-rf', '--one-file-system', '--', '/etc/ceph/',
            ]
        )

        distro.conn.exit()


class StoreVersion(argparse.Action):
    """
    Like ``"store"`` but also remember which one of the exclusive
    options was set.

    There are three kinds of versions: stable, testing and dev.
    This sets ``version_kind`` to be the right one of the above.

    This kludge essentially lets us differentiate explicitly set
    values from defaults.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        if self.dest == 'release':
            self.dest = 'stable'
        namespace.version_kind = self.dest


@priority(20)
def make(parser):
    """
    Install Ceph packages on remote hosts.
    """

    version = parser.add_mutually_exclusive_group()

    version.set_defaults(
        func=install,
        stable=None,  # XXX deprecated in favor of release
        release='firefly',
        dev='master',
        version_kind='stable',
        adjust_repos=False,
    )

    parser.add_argument(
        'host',
        metavar='HOST',
        nargs='+',
        help='hosts to install on',
    )

    parser.add_argument(
        '--local-mirror',
        nargs='?',
        const='PATH',
        default=None,
        help='Fetch packages and push them to hosts for a local repo mirror',
    )

    parser.set_defaults(
        func=install,
    )


@priority(80)
def make_uninstall(parser):
    """
    Remove Ceph packages from remote hosts.
    """
    parser.add_argument(
        'host',
        metavar='HOST',
        nargs='+',
        help='hosts to uninstall Ceph from',
        )
    parser.set_defaults(
        func=uninstall,
        )


@priority(80)
def make_purge(parser):
    """
    Remove Ceph packages from remote hosts and purge all data.
    """
    parser.add_argument(
        'host',
        metavar='HOST',
        nargs='+',
        help='hosts to purge Ceph from',
        )
    parser.set_defaults(
        func=purge,
        )


@priority(80)
def make_purge_data(parser):
    """
    Purge (delete, destroy, discard, shred) any Ceph data from /var/lib/ceph
    """
    parser.add_argument(
        'host',
        metavar='HOST',
        nargs='+',
        help='hosts to purge Ceph data from',
        )
    parser.set_defaults(
        func=purgedata,
        )
