#!/usr/bin/env python3
"""
Coimbatore Ride Pulse - synthetic ride-hailing data generator.

Makes one CSV per month of simulated ride requests over 30 real Coimbatore
areas (approximate area-centre coordinates). Demand follows hour-of-day
profiles per area type, weekends, rain, festivals and yearly growth. Driver
supply follows a shift curve, so evening / late-night shortages and midday
surpluses appear naturally. About 0.4% of rows are deliberately dirty so the
Hadoop cleaning job has real work to do.

Only needs Python 3 + numpy.

    python3 generate_rides.py --out /staging/raw                 # 2 years, ~25k rides/day (~18M rows)
    python3 generate_rides.py --out ./raw --rides-per-day 3000   # small test set
"""
import argparse
import csv
import math
import os
import sys
import time
from datetime import date, timedelta

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))

HEADER = ("ride_id,request_time,pickup_time,dropoff_time,pickup_zone,drop_zone,"
          "pickup_lat,pickup_lng,drop_lat,drop_lng,distance_km,fare,surge,"
          "status,drivers_nearby,weather,festival")

# Relative requests per hour (index = hour of day) for each area type.
PROFILES = {
    "commercial":  [0.25, 0.15, 0.10, 0.08, 0.10, 0.20, 0.45, 0.80, 1.10, 1.30, 1.40, 1.50,
                    1.50, 1.40, 1.30, 1.35, 1.55, 1.90, 2.30, 2.40, 2.10, 1.60, 1.00, 0.55],
    "transit":     [0.55, 0.40, 0.35, 0.50, 1.00, 1.40, 1.60, 1.60, 1.50, 1.30, 1.10, 1.00,
                    1.00, 1.00, 1.05, 1.15, 1.30, 1.60, 1.90, 2.00, 1.85, 1.60, 1.30, 0.90],
    "it_college":  [0.45, 0.30, 0.20, 0.15, 0.15, 0.30, 0.80, 1.80, 2.30, 1.70, 0.90, 0.80,
                    0.90, 0.85, 0.80, 0.90, 1.30, 2.20, 2.50, 2.10, 1.50, 1.20, 1.00, 0.70],
    "residential": [0.20, 0.10, 0.08, 0.08, 0.15, 0.40, 0.90, 1.50, 1.70, 1.30, 0.90, 0.80,
                    0.75, 0.70, 0.70, 0.80, 1.00, 1.30, 1.50, 1.50, 1.30, 0.90, 0.55, 0.35],
    "industrial":  [0.40, 0.20, 0.15, 0.20, 0.50, 1.40, 1.90, 1.40, 1.20, 0.90, 0.80, 0.80,
                    0.90, 1.30, 1.70, 1.20, 0.90, 1.00, 1.10, 1.00, 0.80, 1.20, 1.50, 0.80],
    "airport":     [1.20, 0.60, 0.40, 0.50, 1.10, 1.90, 1.60, 1.00, 0.90, 1.60, 1.70, 1.10,
                    0.90, 1.40, 1.50, 1.00, 0.90, 1.10, 1.50, 1.90, 2.00, 1.60, 1.60, 1.70],
}
WEEKEND_TYPE = {"commercial": 1.25, "transit": 1.10, "it_college": 0.55,
                "residential": 1.00, "industrial": 0.60, "airport": 1.10}
# Weekend shape: lazy mornings, busier afternoons and late evenings.
WEEKEND_HOUR = np.array([1.3, 1.3, 1.2, 1.0, 0.8, 0.7, 0.6, 0.6, 0.65, 0.85, 1.1, 1.2,
                         1.2, 1.2, 1.15, 1.1, 1.1, 1.05, 1.1, 1.2, 1.25, 1.25, 1.25, 1.25])
# Drivers on the road by hour (relative). Many drivers work daytime shifts.
SUPPLY_CURVE = np.array([0.42, 0.30, 0.25, 0.25, 0.35, 0.55, 0.80, 0.95, 1.05, 1.20, 1.30, 1.35,
                         1.35, 1.35, 1.30, 1.25, 1.15, 1.05, 1.00, 0.95, 0.85, 0.70, 0.55, 0.48])
# Average road speed (km/h) by hour.
SPEED = np.array([32, 34, 35, 35, 33, 28, 22, 17, 15, 17, 21, 22, 22, 22, 21, 20, 18, 15, 14,
                  15, 18, 23, 27, 30], dtype=float)
RAIN_PROB_BY_MONTH = {1: .05, 2: .03, 3: .05, 4: .20, 5: .30, 6: .25, 7: .25, 8: .25,
                      9: .30, 10: .50, 11: .55, 12: .30}

# date -> (festival name, {area type: demand multiplier}, supply multiplier, hours affected)
FESTIVALS = {}
def _fest(days, name, demand, supply, hours=range(24)):
    for d in days:
        FESTIVALS[d] = (name, demand, supply, set(hours))

for y in (2024, 2025):
    _fest([date(y, 1, 14), date(y, 1, 15), date(y, 1, 16)], "Pongal",
          {"transit": 1.6, "airport": 1.3, "default": 0.70}, 0.70)
    _fest([date(y, 4, 14)], "Tamil_New_Year", {"commercial": 1.25, "default": 1.0}, 0.90)
    _fest([date(y, 12, 25)], "Christmas", {"commercial": 1.35, "default": 1.05}, 0.85)
    _fest([date(y, 12, 31)], "New_Year_Eve", {"commercial": 1.9, "transit": 1.4, "default": 1.2},
          0.80, hours=range(17, 24))
_fest([date(2024, 9, 7)], "Vinayagar_Chathurthi", {"residential": 1.25, "default": 1.05}, 0.85)
_fest([date(2025, 8, 27)], "Vinayagar_Chathurthi", {"residential": 1.25, "default": 1.05}, 0.85)
_fest([date(2024, 10, 11)], "Ayudha_Pooja", {"industrial": 0.40, "commercial": 1.35, "default": 1.0}, 0.80)
_fest([date(2025, 10, 1)], "Ayudha_Pooja", {"industrial": 0.40, "commercial": 1.35, "default": 1.0}, 0.80)
_fest([date(2024, 10, 30), date(2024, 10, 31)], "Deepavali",
      {"commercial": 1.8, "transit": 1.5, "default": 1.15}, 0.75)
_fest([date(2025, 10, 19), date(2025, 10, 20)], "Deepavali",
      {"commercial": 1.8, "transit": 1.5, "default": 1.15}, 0.75)


def load_zones():
    with open(os.path.join(HERE, "zones.csv"), newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["lat"] = float(r["lat"]); r["lng"] = float(r["lng"])
        r["demand_weight"] = float(r["demand_weight"]); r["supply_ratio"] = float(r["supply_ratio"])
    return rows


def haversine_km(lat1, lng1, lat2, lng2):
    lat1, lng1, lat2, lng2 = map(np.radians, (lat1, lng1, lat2, lng2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lng2 - lng1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


class Generator:
    def __init__(self, rides_per_day, seed):
        self.rng = np.random.default_rng(seed)
        self.zones = load_zones()
        self.nz = len(self.zones)
        self.names = [z["zone"] for z in self.zones]
        self.types = [z["type"] for z in self.zones]
        self.lat = np.array([z["lat"] for z in self.zones])
        self.lng = np.array([z["lng"] for z in self.zones])
        w = np.array([z["demand_weight"] for z in self.zones])
        ratio = np.array([z["supply_ratio"] for z in self.zones])
        prof = np.array([PROFILES[t] for t in self.types])            # (nz, 24)
        self.base = w[:, None] * prof                                   # weekday shape
        self.base *= rides_per_day / self.base.sum()                    # ~rides/day on a weekday
        # Driver capacity per zone-hour, tied to the zone's average weekday demand.
        mean_dem = self.base.mean(axis=1)
        self.supply = (ratio * mean_dem * 1.55)[:, None] * (SUPPLY_CURVE / SUPPLY_CURVE.mean())[None, :]
        # Drop-off choice: gravity model.
        d = haversine_km(self.lat[:, None], self.lng[:, None], self.lat[None, :], self.lng[None, :])
        attract = w.copy()
        for i, t in enumerate(self.types):
            if t in ("airport", "transit"):
                attract[i] *= 1.5
        g = attract[None, :] / (d + 1.5) ** 1.3
        np.fill_diagonal(g, np.diag(g) * 0.15)
        self.drop_cum = np.cumsum(g / g.sum(axis=1, keepdims=True), axis=1)
        self.drop_cum[:, -1] = 1.0
        # Time string tables.
        self.hms = ["%02d:%02d:%02d" % (s // 3600, (s // 60) % 60, s % 60) for s in range(86400)]

    def day_rides(self, day):
        rng = self.rng
        weekend = day.weekday() >= 5
        month = day.month
        growth = 1.0 if day.year == 2024 else 1.12          # demand grows faster than supply
        supply_growth = 1.0 if day.year == 2024 else 1.06
        dow = {4: 1.08, 5: 1.10, 6: 0.95}.get(day.weekday(), 1.0)
        lam = self.base.copy()
        if weekend:
            lam *= np.array([WEEKEND_TYPE[t] for t in self.types])[:, None] * WEEKEND_HOUR[None, :]
        if month == 5:   # summer vacation: colleges quiet
            lam[[i for i, t in enumerate(self.types) if t == "it_college"]] *= 0.7
        lam *= growth * dow
        sup = self.supply * supply_growth

        # Weather: rainy days get a 2-5 hour rain window, mostly afternoon/evening.
        rain_h = np.zeros(24, dtype=bool)
        if rng.random() < RAIN_PROB_BY_MONTH[month]:
            start = int(rng.choice(np.arange(24), p=self._rain_start_p()))
            dur = int(rng.integers(2, 6))
            rain_h[[(start + k) % 24 for k in range(dur)]] = True
        lam[:, rain_h] *= 1.35
        sup[:, rain_h] *= 0.78

        fest_name = "none"
        fest_h = np.zeros(24, dtype=bool)
        if day in FESTIVALS:
            fest_name, dem, smul, hours = FESTIVALS[day]
            fest_h[list(hours)] = True
            for i, t in enumerate(self.types):
                lam[i, fest_h] *= dem.get(t, dem["default"])
            sup[:, fest_h] *= smul

        pressure = lam / sup * rng.lognormal(0, 0.10, size=lam.shape)
        counts = rng.poisson(lam)
        n = int(counts.sum())
        if n == 0:
            return []

        flat = counts.ravel()
        cell = np.repeat(np.arange(flat.size), flat)
        oz = cell // 24
        hr = cell % 24
        p = pressure.ravel()[cell] * rng.lognormal(0, 0.08, size=n)
        req = hr * 3600 + rng.integers(0, 3600, size=n)
        order = np.argsort(req, kind="stable")
        oz, hr, p, req = oz[order], hr[order], p[order], req[order]

        u = rng.random(n)
        dz = (self.drop_cum[oz] < u[:, None]).sum(axis=1)
        plat = self.lat[oz] + rng.normal(0, 0.006, n)
        plng = self.lng[oz] + rng.normal(0, 0.006, n)
        dlat = self.lat[dz] + rng.normal(0, 0.006, n)
        dlng = self.lng[dz] + rng.normal(0, 0.006, n)
        dist = haversine_km(plat, plng, dlat, dlng) * 1.35 + 0.4

        p_nd = np.clip(0.015 + 0.30 * np.maximum(0, p - 0.95) ** 1.1, 0, 0.42)
        p_cancel = 0.03 + 0.03 * np.minimum(p, 2.0)
        r = rng.random(n)
        status = np.where(r < p_nd, 1, np.where(r < p_nd + p_cancel, 2, 0))   # 0 ok, 1 no driver, 2 cancel

        wait_mean = np.minimum(2.5 + 3.2 * p ** 1.6, 25.0)
        wait = rng.gamma(3.0, wait_mean / 3.0) * 60.0
        wait = np.clip(wait, 45, 55 * 60).astype(np.int64)
        rain_r = rain_h[hr]
        speed = SPEED[hr] * np.where(rain_r, 0.8, 1.0)
        trip = (dist / speed * 60.0 * rng.lognormal(0, 0.15, n) + 2.0) * 60.0
        trip = trip.astype(np.int64)
        surge = np.clip(np.round(1 + 0.45 * np.maximum(0, p - 0.9), 1), 1.0, 2.5)
        fare = (55 + 14 * dist + 1.5 * trip / 60.0) * surge
        drivers = rng.poisson(np.maximum(0.3, 6.0 / (p + 0.15) - 1.5))
        fest_r = fest_h[hr]

        return dict(n=n, oz=oz, dz=dz, req=req, wait=wait, trip=trip, plat=plat, plng=plng,
                    dlat=dlat, dlng=dlng, dist=dist, fare=fare, surge=surge, status=status,
                    drivers=drivers, rain=rain_r, fest=fest_r, fest_name=fest_name)

    @staticmethod
    def _rain_start_p():
        w = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4, 6, 8, 9, 9, 8, 6, 4, 3, 2, 1, 1], float)
        return w / w.sum()

    def format_day(self, day, d, seq_start, dirty_rate):
        rng = self.rng
        ds = day.isoformat()
        ds_next = (day + timedelta(days=1)).isoformat()
        hms = self.hms
        names = self.names
        st_names = ("completed", "no_driver", "cancelled")
        yy = day.year % 100

        def ts(sec):
            return (ds if sec < 86400 else ds_next) + " " + hms[sec % 86400]

        lines = []
        oz, dz, req, wait, trip = (d[k].tolist() for k in ("oz", "dz", "req", "wait", "trip"))
        plat, plng, dlat, dlng = (np.round(d[k], 4).tolist() for k in ("plat", "plng", "dlat", "dlng"))
        dist = np.round(d["dist"], 2).tolist()
        fare = np.round(d["fare"], 2).tolist()
        surge = d["surge"].tolist()
        status = d["status"].tolist()
        drivers = d["drivers"].tolist()
        rain = d["rain"].tolist()
        fest = d["fest"].tolist()
        fname = d["fest_name"]
        dirty = set(np.nonzero(rng.random(d["n"]) < dirty_rate)[0].tolist())

        for i in range(d["n"]):
            s = status[i]
            rq = req[i]
            if s == 0:
                pu = rq + wait[i]
                pick, drop, f, sg = ts(pu), ts(pu + trip[i]), fare[i], surge[i]
            else:
                pick, drop, f, sg = "", "", 0.0, surge[i]
            line = "R%02d%08d,%s %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s" % (
                yy, seq_start + i, ds, hms[rq], pick, drop, names[oz[i]], names[dz[i]],
                plat[i], plng[i], dlat[i], dlng[i], dist[i], f, sg, st_names[s], drivers[i],
                "rain" if rain[i] else "clear", fname if fest[i] else "none")
            if i in dirty:
                line = self._corrupt(line)
            lines.append(line)
        return lines

    def _corrupt(self, line):
        f = line.split(",")
        k = self.rng.integers(0, 5)
        if k == 0:                      # GPS glitch
            f[6], f[7] = "0.0", "0.0"
        elif k == 1:                    # negative fare from a refund bug
            f[11] = "-" + f[11].lstrip("-") if f[11] not in ("0.0",) else "-35.0"
        elif k == 2 and f[2]:           # pickup logged before request (clock skew)
            f[2] = f[1][:11] + "00:00:00" if f[1][11:13] != "00" else f[2]
            if f[2] >= f[1]:
                f[2] = ""
                f[13] = "unknown"
        elif k == 3:                    # truncated upload
            f = f[:9]
        else:                           # bad status value
            f[13] = "UNKNOWN"
        return ",".join(f)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="output folder for monthly CSV files")
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--rides-per-day", type=int, default=25000,
                    help="average weekday requests per day in 2024 (default 25000 -> ~18M rows)")
    ap.add_argument("--dirty-rate", type=float, default=0.004)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    gen = Generator(args.rides_per_day, args.seed)
    day = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    total, t0 = 0, time.time()
    seq = {}
    cur_key, fh = None, None
    while day <= end:
        key = (day.year, day.month)
        if key != cur_key:
            if fh:
                fh.close()
            path = os.path.join(args.out, "rides_%04d_%02d.csv" % key)
            fh = open(path, "w", newline="\n")
            fh.write(HEADER + "\n")
            cur_key = key
            print("  writing %s" % path, flush=True)
        d = gen.day_rides(day)
        if d:
            s = seq.get(day.year, 0)
            lines = gen.format_day(day, d, s, args.dirty_rate)
            seq[day.year] = s + d["n"]
            fh.write("\n".join(lines))
            fh.write("\n")
            total += d["n"]
        day += timedelta(days=1)
    if fh:
        fh.close()
    dt = time.time() - t0
    print("done: %d rides in %.0fs (%.0f rows/s)" % (total, dt, total / max(dt, 1e-9)))


if __name__ == "__main__":
    sys.exit(main())
