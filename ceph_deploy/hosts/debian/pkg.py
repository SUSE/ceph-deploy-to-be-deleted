from ceph_deploy.util import pkg_managers


def install(distro, packages):
    return pkg_managers.apt(
        distro.conn,
        packages
    )


def remove(distro, packages):
    return pkg_managers.apt_remove(
        distro.conn,
        packages
    )

def update(distro):
    return pkg_managers.apt_update(
        distro.conn
    )
