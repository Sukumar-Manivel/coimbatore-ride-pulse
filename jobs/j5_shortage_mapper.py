#!/usr/bin/env python3
"""Job 5 - Driver shortage index (chained: reads Job 2 output).
Re-emits each zone|daytype|hour|weather row and also an 'all' weather row, so the
reducer can score both the weather-specific and the overall situation."""
import sys

def main():
    out = sys.stdout.write
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        key, _, val = line.partition("\t")
        zone, dt, hr, weather = key.split("|")
        out("%s\t%s\n" % (key, val))
        out("%s|%s|%s|all\t%s\n" % (zone, dt, hr, val))

if __name__ == "__main__":
    main()
