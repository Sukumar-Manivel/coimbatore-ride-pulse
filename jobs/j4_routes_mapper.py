#!/usr/bin/env python3
"""Job 4 - Busy routes (origin -> destination) per time band.
key   = pickup_zone|drop_zone|band     (band: night, morning, midday, evening, late)
value = requests,completed,no_driver,wait_sec_sum,fare_sum,distance_sum
Run with KeyFieldBasedPartitioner on field 1 so ALL routes from one origin land on the
same reducer, which can then pick that origin's top destinations."""
import sys
from rideparse import parse, wait_seconds

BANDS = ["night"] * 6 + ["morning"] * 5 + ["midday"] * 5 + ["evening"] * 5 + ["late"] * 3

def main():
    out = sys.stdout.write
    for line in sys.stdin:
        f, bad = parse(line)
        if bad:
            continue
        st = f[13]
        comp = st == "completed"
        w = wait_seconds(f[1], f[2]) if comp else 0
        out("%s|%s|%s\t1,%d,%d,%d,%s,%s\n" % (
            f[4], f[5], BANDS[int(f[1][11:13])], comp, st == "no_driver", w,
            f[11] if comp else "0", f[10]))

if __name__ == "__main__":
    main()
