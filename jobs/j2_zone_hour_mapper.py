#!/usr/bin/env python3
"""Job 2 - Zone x day-type x hour x weather demand / supply stats.
key   = pickup_zone|weekday-or-weekend|hour|weather
value = requests,completed,no_driver,cancelled,wait_sec_sum,long_waits(>10min),
        drivers_nearby_sum,fare_sum,surge_sum
Safe on raw input too (it re-validates), which the benchmarks rely on."""
import sys
from rideparse import parse, counter, day_type, wait_seconds

def main():
    out = sys.stdout.write
    skipped = 0
    for line in sys.stdin:
        f, bad = parse(line)
        if bad:
            skipped += 1
            continue
        req = f[1]
        st = f[13]
        comp = st == "completed"
        w = wait_seconds(req, f[2]) if comp else 0
        key = "%s|%s|%s|%s" % (f[4], day_type(req[:10]), req[11:13], f[15])
        out("%s\t1,%d,%d,%d,%d,%d,%s,%s,%s\n" % (
            key, comp, st == "no_driver", st == "cancelled", w, w > 600,
            f[14], f[11] if comp else "0", f[12]))
    if skipped:
        counter("DataQuality", "SkippedInJob2", skipped)

if __name__ == "__main__":
    main()
