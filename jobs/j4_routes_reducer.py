#!/usr/bin/env python3
"""Job 4 reducer: sums each route, then keeps the TOP_N busiest destinations per
(origin, band). Works because the partitioner sends one origin to one reducer and
keys of one origin arrive together (they share the key prefix)."""
import sys

TOP_N = 6

def fmt(x):
    r = round(x, 2)
    return str(int(r)) if r == int(r) else repr(r)

def flush(origin, routes):
    by_band = {}
    for (dz, band), v in routes.items():
        by_band.setdefault(band, []).append((v[0], dz, v))
    for band, lst in by_band.items():
        lst.sort(reverse=True)
        for _, dz, v in lst[:TOP_N]:
            sys.stdout.write("%s|%s|%s\t%s\n" % (origin, dz, band, ",".join(fmt(x) for x in v)))

def main():
    origin, routes = None, {}
    for line in sys.stdin:
        key, _, val = line.rstrip("\n").partition("\t")
        pz, dz, band = key.split("|")
        nums = [float(x) for x in val.split(",")]
        if pz != origin:
            if origin is not None:
                flush(origin, routes)
            origin, routes = pz, {}
        acc = routes.get((dz, band))
        if acc is None:
            routes[(dz, band)] = nums
        else:
            for i, x in enumerate(nums):
                acc[i] += x
    if origin is not None:
        flush(origin, routes)

if __name__ == "__main__":
    main()
