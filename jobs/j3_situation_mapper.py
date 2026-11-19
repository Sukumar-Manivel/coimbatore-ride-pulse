#!/usr/bin/env python3
"""Job 3 - Situations & time series (many keys per ride -> the combiner matters a lot).
Every value has the same 10 fields:
requests,completed,no_driver,cancelled,wait_sec_sum,long_waits,drivers_sum,fare_sum,dropoffs,rain_requests
Key families:
  Y|year                 yearly totals
  YZ|year|zone           yearly totals per pickup zone
  H|daytype|hour         requests by request hour
  DO|daytype|hour        drop-offs by drop-off hour
  W|weather|hour         rain vs clear by hour
  F|festival|hour        festival days by hour
  D|yyyy-mm-dd           daily series
  M|yyyy-mm              monthly series"""
import sys
from rideparse import parse, day_type, wait_seconds

def main():
    out = sys.stdout.write
    for line in sys.stdin:
        f, bad = parse(line)
        if bad:
            continue
        req = f[1]
        ds = req[:10]
        dt = day_type(ds)
        hr = req[11:13]
        st = f[13]
        comp = st == "completed"
        w = wait_seconds(req, f[2]) if comp else 0
        rain = f[15] == "rain"
        v = "1,%d,%d,%d,%d,%d,%s,%s,0,%d" % (
            comp, st == "no_driver", st == "cancelled", w, w > 600, f[14],
            f[11] if comp else "0", rain)
        out("Y|%s\t%s\n" % (ds[:4], v))
        out("YZ|%s|%s\t%s\n" % (ds[:4], f[4], v))
        out("H|%s|%s\t%s\n" % (dt, hr, v))
        out("W|%s|%s\t%s\n" % (f[15], hr, v))
        out("D|%s\t%s\n" % (ds, v))
        out("M|%s\t%s\n" % (ds[:7], v))
        if f[16] != "none":
            out("F|%s|%s\t%s\n" % (f[16], hr, v))
        if comp:
            out("DO|%s|%s\t0,0,0,0,0,0,0,0,1,0\n" % (day_type(f[3][:10]), f[3][11:13]))

if __name__ == "__main__":
    main()
