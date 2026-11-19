#!/usr/bin/env python3
"""Job 6 - Global hotspot ranking (chained: reads Job 5 output).
Turns the shortage index into the sort key (1000 - index, zero padded) so Hadoop's
shuffle sort does the ranking. One reducer (identity 'cat') => one totally ordered list."""
import sys

def main():
    for line in sys.stdin:
        key, _, val = line.rstrip("\n").partition("\t")
        if not key.endswith("|all"):
            continue
        zone, dt, hr, _ = key.split("|")
        idx = float(val.split(",", 1)[0])
        sys.stdout.write("%07.2f\t%s|%s|%s,%s\n" % (1000.0 - idx, zone, dt, hr, val))

if __name__ == "__main__":
    main()
