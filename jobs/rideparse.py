"""Shared parsing / validation for all MapReduce jobs (shipped to every task with -files)."""
import sys
from datetime import date

NFIELDS = 17
STATUSES = ("completed", "no_driver", "cancelled")
LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX = 10.85, 11.20, 76.80, 77.20
_dow_cache = {}


def counter(group, name, n=1):
    """Hadoop Streaming counter: written to stderr, picked up by the framework."""
    sys.stderr.write("reporter:counter:%s,%s,%d\n" % (group, name, n))


def secs(ts):
    return int(ts[11:13]) * 3600 + int(ts[14:16]) * 60 + int(ts[17:19])


def day_type(ds):
    """'weekday' or 'weekend' for a YYYY-MM-DD string."""
    v = _dow_cache.get(ds)
    if v is None:
        v = "weekend" if date(int(ds[0:4]), int(ds[5:7]), int(ds[8:10])).weekday() >= 5 else "weekday"
        _dow_cache[ds] = v
    return v


def wait_seconds(req, pick):
    w = secs(pick) - secs(req)
    if pick[:10] != req[:10]:
        w += 86400
    return w


def parse(line):
    """Return (fields, None) for a valid ride, or (None, reason) for a bad one."""
    line = line.rstrip("\r\n")
    if not line:
        return None, "Empty"
    if line.startswith("ride_id"):
        return None, "Header"
    f = line.split(",")
    if len(f) != NFIELDS:
        return None, "BadFieldCount"
    if f[13] not in STATUSES:
        return None, "BadStatus"
    try:
        lat, lng = float(f[6]), float(f[7])
        fare = float(f[11])
        dist = float(f[10])
    except ValueError:
        return None, "Unparseable"
    if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
        return None, "BadCoords"
    if fare < 0:
        return None, "NegativeFare"
    if f[13] == "completed":
        if not f[2] or not f[3] or dist <= 0:
            return None, "MissingTrip"
        w = wait_seconds(f[1], f[2])
        if w < 0 or w > 3600:
            return None, "PickupBeforeRequest"
    return f, None
