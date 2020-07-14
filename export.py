#!/usr/bin/python3

from typing import List, Tuple
from enum import Enum, auto
import argparse
from zpool_parser import get_zpool_status, ZPoolState, DriveStatus, SubpoolType, SubpoolStatus, ZPoolStatus


def export_zfs_text(pool_data: List[ZPoolStatus]):
    return export_zfs_pool_health(pool_data) \
        + export_zfs_drive_health(pool_data) \
        + export_zfs_resilver_status(pool_data) \
        + export_zfs_resilver_time(pool_data) \
        + export_zfs_resilver_last_time(pool_data) \
        + export_zfs_scrub_status(pool_data) \
        + export_zfs_scrub_time(pool_data) \
        + export_zfs_scrub_last_time(pool_data)


def export_zfs_pool_health(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Pool_Health: 0=healthy, 1=degraded\n"
              "# TYPE ZFS_Pool_Health gauge\n")

    for pool in pool_data:
        export += "ZFS_Pool_Health{{pool=\"{0}\"}} {1}\n".format(
            pool.name, pool.state.value)

    return export + "\n"


def export_zfs_drive_health(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Drive_Health: 0=healthy, 1=degraded, 2=unavail\n"
              "# TYPE ZFS_Drive_Health gauge\n")

    for pool in pool_data:
        for subpool in pool.subpools:
            for drive in subpool.drives:
                export += "ZFS_Drive_Health{{pool=\"{0}\", name=\"{1}\"}} {2}\n".format(
                    pool.name, drive.name, drive.state.value)

    return export + "\n"


def export_zfs_resilver_status(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Resilver_Status: 0=not resilvering, 1=resilvering\n"
              "# TYPE ZFS_Resilver_Status gauge\n")

    for pool in pool_data:
        export += "ZFS_Resilver_Status{{pool=\"{0}\"}} {1}\n".format(
            pool.name, 1 if pool.currently_resilvering else 0)
    
    return export + "\n"


def export_zfs_resilver_time(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Resilver_Time_Remaining: time in seconds\n"
              "# TYPE ZFS_Resilver_Time_Remaining gauge\n")

    for pool in pool_data:
        export += "ZFS_Resilver_Time_Remaining{{pool=\"{0}\"}} {1}\n".format(
            pool.name, pool.resilver_time_remaining)

    return export + "\n"


def export_zfs_resilver_last_time(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Resilver_Last_Time: time since epoch\n"
              "# TYPE ZFS_Resilver_Last_Time gauge\n")

    for pool in pool_data:
        export += "ZFS_Resilver_Last_Time{{pool=\"{0}\"}} {1}\n".format(
            pool.name, pool.last_resilver)

    return export + "\n"


def export_zfs_scrub_status(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Scrub_Status: 0=not scrubbing, 1=scrubbing\n"
              "# TYPE ZFS_Scrub_Status gauge\n")

    for pool in pool_data:
        export += "ZFS_Scrub_Status{{pool=\"{0}\"}} {1}\n".format(
            pool.name, 1 if pool.currently_scrubbing else 0)
    
    return export + "\n"


def export_zfs_scrub_time(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Scrub_Time_Remaining: time in seconds\n"
              "# TYPE ZFS_Scrub_Time_Remaining gauge\n")

    for pool in pool_data:
        export += "ZFS_Scrub_Time_Remaining{{pool=\"{0}\"}} {1}\n".format(
            pool.name, pool.scrub_time_remaining)

    return export + "\n"


def export_zfs_scrub_last_time(pool_data: List[ZPoolStatus]) -> str:
    export = ("# HELP ZFS_Scrub_Last_Time: time since epoch\n"
              "# TYPE ZFS_Scrub_Last_Time gauge\n")

    for pool in pool_data:
        export += "ZFS_Scrub_Last_Time{{pool=\"{0}\"}} {1}\n".format(
            pool.name, pool.last_scrub)

    return export + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Prometheus formatted list.")
    parser.add_argument("-o", "--output-file", type=str, help="path + filename to output to")
    args = parser.parse_args()
    # print(args)
    
    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(export_zfs_text(get_zpool_status()))
    else:
        print(export_zfs_text(get_zpool_status()))
