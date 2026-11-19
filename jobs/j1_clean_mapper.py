#!/usr/bin/env python3
"""Job 1 - Clean (map-only).
Drops broken rows and counts each problem with Hadoop counters (group 'DataQuality').
Output: the same CSV line, only valid rows."""
import sys
from rideparse import parse, counter

def main():
    stats = {}
    for line in sys.stdin:
        f, bad = parse(line)
        if bad:
            stats[bad] = stats.get(bad, 0) + 1
            continue
        stats["ValidRows"] = stats.get("ValidRows", 0) + 1
        sys.stdout.write(",".join(f) + "\n")
    for k, v in stats.items():
        counter("DataQuality", k, v)

if __name__ == "__main__":
    main()
