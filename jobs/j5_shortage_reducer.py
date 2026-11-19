#!/usr/bin/env python3
"""Job 5 reducer: sums Job-2 vectors per key and scores the driver situation.

shortage index (0-100) =
    45 * min(no_driver_rate / 0.25, 1)          riders who got no driver at all
  + 35 * min(max(avg_wait_min - 3, 0) / 12, 1)  how long riders waited
  + 20 * max(0, 1 - drivers_nearby / 4)          idle drivers near the rider at request time
status: need_drivers if index >= 50
        surplus      if drivers_nearby >= 7 and avg_wait < 4.5 min and no_driver_rate < 3%
        balanced     otherwise
Output: key<TAB>index,status,requests,no_driver_rate,avg_wait_min,long_wait_rate,
        avg_drivers_nearby,avg_surge,completed,no_driver"""
import sys

def score(v):
    req, comp, nd, canc, wsum, wlong, drv, fare, surge = v
    nd_rate = nd / req if req else 0.0
    avg_wait = wsum / comp / 60.0 if comp else 0.0
    long_rate = wlong / comp if comp else 0.0
    dn = drv / req if req else 0.0
    sg = surge / req if req else 1.0
    idx = (45 * min(nd_rate / 0.25, 1.0)
           + 35 * min(max(avg_wait - 3.0, 0.0) / 12.0, 1.0)
           + 20 * max(0.0, 1.0 - dn / 4.0))
    if idx >= 50:
        status = "need_drivers"
    elif dn >= 7 and avg_wait < 4.5 and nd_rate < 0.03:
        status = "surplus"
    else:
        status = "balanced"
    return "%.1f,%s,%d,%.4f,%.2f,%.4f,%.2f,%.3f,%d,%d" % (
        idx, status, req, nd_rate, avg_wait, long_rate, dn, sg, comp, nd)

def main():
    cur, acc = None, None
    for line in sys.stdin:
        key, _, val = line.rstrip("\n").partition("\t")
        nums = [float(x) for x in val.split(",")]
        if key == cur:
            for i, x in enumerate(nums):
                acc[i] += x
        else:
            if cur is not None:
                sys.stdout.write(cur + "\t" + score(acc) + "\n")
            cur, acc = key, nums
    if cur is not None:
        sys.stdout.write(cur + "\t" + score(acc) + "\n")

if __name__ == "__main__":
    main()
