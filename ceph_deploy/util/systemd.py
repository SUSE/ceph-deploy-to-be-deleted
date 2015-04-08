# This module groups to gether build up and tear down of systemd service manipulation.

import logging
from ceph_deploy.lib import remoto

# local constants / variables

systemd_service_path_local = '/etc/systemd/system/'
LOG = logging.getLogger(__name__)


service_content_mon = """[Unit]
Description=Ceph cluster monitor daemon
After=network-online.target local-fs.target
Wants=network-online.target local-fs.target
PartOf=ceph.target

[Service]
EnvironmentFile=-/etc/sysconfig/ceph
Environment=CLUSTER={cluster}
ExecStart=/usr/bin/ceph-mon -f --cluster ${{CLUSTER}} --id %i

[Install]
WantedBy=ceph.target
"""

service_content_osd = """[Unit]
Description=Ceph object storage daemon
After=network-online.target local-fs.target
Wants=network-online.target local-fs.target
PartOf=ceph.target

[Service]
EnvironmentFile=-/etc/sysconfig/ceph
Environment=CLUSTER={cluster}
ExecStart=/usr/bin/ceph-osd -f --cluster ${{CLUSTER}} --id %i
ExecStartPre=/usr/lib/ceph/ceph-osd-prestart.sh --cluster ${{CLUSTER}} --id %i

[Install]
WantedBy=ceph.target"""

service_content_mds = """[Unit]
Description=Ceph object storage daemon
After=network-online.target local-fs.target
Wants=network-online.target local-fs.target
PartOf=ceph.target

[Service]
EnvironmentFile=-/etc/sysconfig/ceph
Environment=CLUSTER={cluster}
ExecStart=/usr/bin/ceph-osd -f --cluster ${{CLUSTER}} --id %i
ExecStartPre=/usr/lib/ceph/ceph-osd-prestart.sh --cluster ${{CLUSTER}} --id %i

[Install]
WantedBy=ceph.target"""



def generate_service_mon_path(clustername):
    return "ceph-%s-mon@.service" % (clustername)

def generate_service_osd_path(clustername):
    return "ceph-%s-osd@.service" % (clustername)

def generate_service_mds_path(clustername):
    return "ceph-%s-mds@.service" % (clustername)

def generate_service_mon_path_full(clustername):
    return "%s/%s" % (systemd_service_path_local, generate_service_mon_path(clustername))

def generate_service_osd_path_full(clustername):
    return "%s/%s" % (systemd_service_path_local, generate_service_osd_path(clustername))

def generate_service_mds_path_full(clustername):
    return "%s/%s" % (systemd_service_path_local, generate_service_mds_path(clustername))



# Top level functions

def restart(distro):
    stdout, stderr, rc = remoto.process.check(
        distro.conn,
        [
            "systemctl",
            "daemon-reload"            
            ],
        )
    if rc != 0:
        LOG.warning("Failed executing 'systemctl daemon-reload'")




def build_up(distro, args):
    """
    Tear down generated files when with systemd custome cluster names.
    """
    if distro.init != 'systemd':
        # dont act on non systemd systems
        return
    if args.cluster == "ceph":
        # dont act on systems witht he default cluster name
        return
    
    changed_service_files = False
    
    mon_serice_path = generate_service_mon_path_full(args.cluster)
    if not distro.conn.remote_module.path_exists(mon_serice_path):
        content = service_content_mon.format(cluster = args.cluster)
        distro.conn.remote_module.write_file(mon_serice_path, content)
    
    osd_serice_path = generate_service_osd_path_full(args.cluster)
    if not distro.conn.remote_module.path_exists(osd_serice_path):
        content = service_content_osd.format(cluster = args.cluster)
        distro.conn.remote_module.write_file(osd_serice_path, content)
    
    mds_serice_path = generate_service_mds_path_full(args.cluster)
    if not distro.conn.remote_module.path_exists(mds_serice_path):
        content = service_content_mds.format(cluster = args.cluster)
        distro.conn.remote_module.write_file(mds_serice_path, content)

    if changed_service_files:
        restart(distro)


def teardown(distro, args):
    """
    Tear down generated files when with systemd custome cluster names.
    """
    if distro.init != 'systemd':
        # dont act on non systemd systems
        return
    if args.cluster == "ceph":
        # dont act on systems witht he default cluster name
        return
    if not distro.conn.remote_module.path_exists(systemd_service_path_local):
        return
    
    changed_service_files = False
    
    mon_serice_path = generate_service_mon_path_full(args.cluster)
    if distro.conn.remote_module.path_exists(mon_serice_path):
        distro.conn.remote_module.unlink(mon_serice_path)
        changed_service_files = True
    
    osd_serice_path = generate_service_osd_path_full(args.cluster)
    if distro.conn.remote_module.path_exists(osd_serice_path):
        distro.conn.remote_module.unlink(osd_serice_path)
        changed_service_files = True
    
    mds_serice_path = generate_service_mds_path_full(args.cluster)
    if distro.conn.remote_module.path_exists(mds_serice_path):
        distro.conn.remote_module.unlink(mds_serice_path)
        changed_service_files = True
    
    if changed_service_files:
        restart(distro)
