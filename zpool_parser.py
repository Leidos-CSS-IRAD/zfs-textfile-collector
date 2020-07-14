#!/usr/bin/python3

import subprocess
from typing import List, Tuple
from enum import Enum, auto
from datetime import datetime
import re

# indices in zpool list:
NAME_INDEX = 0
SIZE_INDEX = 1
ALLOC_INDEX = 2
FREE_INDEX = 3
CKPOINT_INDEX = 4
EXPANDZ_INDEX = 5
FRAG_INDEX = 6
CAP_INDEX = 7
DEDUP_INDEX = 8
HEALTH_INDEX = 9
ALTROOT_INDEX = 10

class ZPoolState(Enum):
    ONLINE = 0
    DEGRADED = 1
    UNAVAIL = 2
    SPARE_INUSE = 3
    SPARE_AVAIL = 4
    OFFLINE = 5
    REMOVED = 6
    FAULTED = 7

class DriveStatus(object):
    name: str
    state: ZPoolState
    is_spare: bool

    def __init__(self, name, spare=False, state=ZPoolState.ONLINE) -> None:
        self.name = name
        self.is_spare = spare
        self.state = state

class SubpoolType(Enum):
    MIRROR = auto()
    RAIDZ = auto()
    RAIDZ2 = auto()

class SubpoolStatus(object):
    pool: SubpoolType
    drives: List[DriveStatus] = []
    state: ZPoolState

    def __init__(self, pool: SubpoolType) -> None:
        self.pool =  pool

class ZPoolStatus(object):
    name = ""
    state = ZPoolState.ONLINE

    last_resilver = 0
    resilver_time_remaining = 0

    last_scrub = 0
    scrub_time_remaining = 0

    currently_resilvering = False
    currently_scrubbing = False

    subpools: List[SubpoolStatus] = []
    spares: List[DriveStatus] = []

    def __repr__(self) -> str:
        return "ZPoolStatus()"
    
    def __str__(self) -> str:
        return self.name + " " + str(self.state)

def get_indent_level(line: str, start_pad: int, indent_amount: int = 2) -> int:
    return (len(line) - len(line.lstrip()) - start_pad) // indent_amount

def state_from_string(string: str) -> ZPoolState:
    if string == "ONLINE":
        return ZPoolState.ONLINE
    elif string == "OFFLINE":
        return ZPoolState.OFFLINE
    elif string == "UNAVAIL":
        return ZPoolState.UNAVAIL
    elif string == "DEGRADED":
        return ZPoolState.DEGRADED
    elif string == "REMOVED":
        return ZPoolState.REMOVED
    elif string == "FAULTED":
        return ZPoolState.FAULTED
    elif string == "INUSE":
        return ZPoolState.SPARE_INUSE
    elif string == "AVAIL":
        return ZPoolState.SPARE_AVAIL
    else:
        print("state didn't match anything...")
        return ZPoolState.ONLINE

def get_zpool_status() -> List[ZPoolStatus]:
    p = subprocess.check_output(["zpool", "status"]).decode("ascii")
    return parse_zpool_status([], p.split("\n"))

def parse_zpool_status(pools: List[ZPoolStatus], remaining_lines: List[str]) -> List[ZPoolStatus]:
    if len(remaining_lines) <= 0:
        return pools

    line = remaining_lines[0]
    # print("line:", line)
    if line.strip().startswith("pool:"):
        pools.append(ZPoolStatus())
        pools[-1].name = line.split(":")[1].strip()
        return parse_zpool_status(pools, remaining_lines[1:])
    elif line.strip().startswith("state:"):
        state_str = line.split(":")[1].strip()
        if state_str == "ONLINE":
            pools[-1].state = ZPoolState.ONLINE
        elif state_str == "DEGRADED":
            pools[-1].state = ZPoolState.DEGRADED
        elif state_str == "UNAVAIL":
            pools[-1].state = ZPoolState.UNAVAIL
        return parse_zpool_status(pools, remaining_lines[1:])
    elif line.strip().startswith("scan:"):
        # TODO: parse resilvering status
        pools[-1], remaining_lines = parse_zpool_status_scan(pools[-1], remaining_lines)
        return parse_zpool_status(pools, remaining_lines)
    elif line.strip().startswith("config:"):
        pools[-1], remaining_lines = parse_zpool_status_config(pools[-1], remaining_lines[1:], 0)
        return parse_zpool_status(pools, remaining_lines)
    elif line.strip().startswith("errors:"):
        # TODO: parse errors string
        return parse_zpool_status(pools, remaining_lines[1:])
    elif line.isspace() or len(line) == 0:
        return parse_zpool_status(pools, remaining_lines[1:])
    else:
        print("something has gone very wrong...", line)
        return pools

def parse_zpool_status_scan(pool: ZPoolStatus, lines: List[str]) -> Tuple[ZPoolStatus, List[str]]:
    line = lines[0]
    line_list = line.split(": ")
    # print("scan line:", line_list)
    if line_list[1].startswith("scrub"):
        if "in progress" in line_list[1]:
            line = lines[2]
            time_string = line.strip().split(", ")[2][:-6]
            matches = list(map(int, re.findall(r"([0-9]+) days ([0-9]+):([0-9]+):([0-9]+)", time_string)[0]))
            pool.currently_scrubbing = True
            pool.scrub_time_remaining = matches[0] * 24 * 60 * 60 + matches[1] * 60 * 60 + matches[2] * 60 + matches[3]
            return pool, lines[3:]
        elif "completed" in line_list[1] or "repaired" in line_list[1]:
            date = line_list[1].split(" on ")[-1].strip()
            pool.currently_scrubbing = False
            pool.last_scrub = int(datetime.strptime(date, "%a %b %d %H:%M:%S %Y").timestamp())
            # print("returning lines", lines[1:])
            return pool, lines[1:]

    elif line_list[1].startswith("resilver"):
        if "in progress" in line_list[1]:
            time_string = line_list[1].split(", ")[-1].strip()[:-6]
            print(re.findall(r"([0-9]+) days ([0-9]+):([0-9]+):([0-9]+)", time_string))
            matches = list(map(int, re.findall(r"([0-9]+) days ([0-9]+):([0-9]+):([0-9]+)", time_string)[0]))
            pool.currently_resilvering = True
            pool.resilver_time_remaining = matches[0] * 24 * 60 * 60 + matches[1] * 60 * 60 + matches[2] * 60 + matches[3]
            return pool, lines[1:]
        elif "completed" in line_list[1] or "repaired" in line_list[1]:
            date = line_list[1].split(" on ")[-1].strip()
            pool.currently_resilvering = False
            pool.last_resilver = int(datetime.strptime(date, "%a %b %d %H:%M:%S %Y").timestamp())
            return pool, lines[1:]

    return pool, lines[1:]

def parse_zpool_status_config(pool: ZPoolStatus, lines: List[str], start_pad: int) -> Tuple[ZPoolStatus, List[str]]:
    line = lines[0]
    # print("config line:", line)
    if line.strip().startswith("errors:"):
        return pool, lines
    elif line.isspace() or len(line) == 0:
        return parse_zpool_status_config(pool, lines[1:], start_pad)
    elif line.strip().startswith("NAME"):
        start_pad = len(line) - len(line.lstrip())
        return parse_zpool_status_config(pool, lines[1:], start_pad)
    elif get_indent_level(line, start_pad) == 0:
        if line.strip().startswith("spares"):
            pool, lines = parse_zpool_config_spares(pool, lines[1:], start_pad)
            return parse_zpool_status_config(pool, lines, start_pad)
        elif line.strip().startswith(pool.name):
            return parse_zpool_status_config(pool, lines[1:], start_pad)
        else:
            print("indent level 0 issues:", line)
            return pool, lines[1:]
    elif get_indent_level(line, start_pad) == 1:
        subpool: SubpoolStatus
        if line.strip().startswith("raidz2"):
            subpool = SubpoolStatus(SubpoolType.RAIDZ2)
        elif line.strip().startswith("raidz"):
            subpool = SubpoolStatus(SubpoolType.RAIDZ)
        elif line.strip().startswith("mirror"):
            subpool = SubpoolStatus(SubpoolType.MIRROR)
        else:
            print("subpool type not matching anything:", line)
            subpool = SubpoolStatus(SubpoolType.MIRROR)
        subpool, lines = parse_zpool_config_subpool(subpool, lines[1:], start_pad)
        pool.subpools.append(subpool)
        return parse_zpool_status_config(pool, lines, start_pad)

    else:
        print("something has gone very wrong in config parsing...", line)
        return pool, lines

def parse_zpool_config_subpool(subpool: SubpoolStatus, lines: List[str], start_pad: int) -> Tuple[SubpoolStatus, List[str]]:
    line = lines[0]
    if get_indent_level(line, start_pad) < 2:
        return subpool, lines
    else:
        line_list = line.split()
        drive = DriveStatus(line_list[0], state=state_from_string(line_list[1]))
        drive.state = state_from_string(line_list[1])
        subpool.drives.append(drive)
        return parse_zpool_config_subpool(subpool, lines[1:], start_pad)

def parse_zpool_config_spares(pool: ZPoolStatus, lines: List[str], start_pad: int) -> Tuple[ZPoolStatus, List[str]]:
    line = lines[0]
    if line.isspace() or len(line) == 0:
        return pool, lines[1:]
    elif get_indent_level(line, start_pad) == 1:
        line_list = line.split()
        drive = DriveStatus(line_list[0], state=state_from_string(line_list[1]), spare=True)
        pool.spares.append(drive)
        return pool, lines[1:]
    else:
        print("spare parsing went wrong...", line)
        return pool, lines




if __name__ == "__main__":
    # p = subprocess.check_output(["zpool", "list", "-Hpv"]).decode("ascii")
    # print(p)
    # arr = [l.strip().split() for l in p.split("\n")]
    # print(arr)
    print(get_zpool_status()[0])