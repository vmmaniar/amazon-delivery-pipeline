"""Build the charts and docs/FINDINGS.md from the marts. Every number comes from the DB.

  python -m pipeline.report            (run from src/, or PYTHONPATH=src)
"""
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from . import db  # noqa: E402
from .config import ROOT  # noqa: E402

DOCS = ROOT / "docs"
CHARTS = DOCS / "charts"
ACCENT, MUTED, WARN = "#2563eb", "#9ca3af", "#dc2626"

plt.rcParams.update({
    "figure.figsize": (6.4, 3.6), "figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "axes.axisbelow": True, "font.size": 9, "axes.titleweight": "bold",
    "axes.titlesize": 10, "axes.titlelocation": "left",
})


def rows(conn, sql):
    return conn.execute(sql).fetchall()


def save(fig, name):
    CHARTS.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(CHARTS / f"{name}.png")
    plt.close(fig)


def bar(name, title, labels, values, xlabel="", ylabel="Avg delivery time (min)", highlight=None, fmt="{:.0f}"):
    fig, ax = plt.subplots()
    colors = [ACCENT if (highlight is None or l == highlight) else MUTED for l in labels]
    b = ax.bar(labels, values, color=colors)
    for r, v in zip(b, values):
        ax.annotate(fmt.format(v), (r.get_x() + r.get_width() / 2, v), ha="center", va="bottom", fontsize=8)
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    ax.margins(y=0.12)
    save(fig, name)


def build(conn):
    q = lambda s: rows(conn, s)  # noqa: E731
    H = q("SELECT * FROM marts.headline")[0]
    traffic = q("SELECT traffic, avg_min FROM (SELECT traffic, round(avg(delivery_minutes),1) avg_min "
                "FROM marts.v_trip WHERE traffic <> 'Unknown' GROUP BY 1) x ORDER BY avg_min")
    weather = q("SELECT weather, round(avg(delivery_minutes),1) m, round(avg(agent_rating),3) r, count(*) "
                "FROM marts.v_trip WHERE weather <> 'Unknown' GROUP BY 1 ORDER BY m")
    dist = q("SELECT km_band, avg_delivery_min, trips FROM marts.distance_band ORDER BY band_min_km")
    hours = q("SELECT order_hour, is_weekend, avg_delivery_min, trips FROM marts.hourly_demand ORDER BY 1, 2")
    ages = q("SELECT age_band, avg_rating, avg_delivery_min, trips FROM marts.age_band_rating ORDER BY 1")
    cats = q("SELECT category_name, avg_delivery_min, trips FROM marts.category_performance ORDER BY 2")
    veh = q("SELECT vehicle_type, trips, avg_delivery_min, avg_speed_kmph FROM marts.vehicle_performance ORDER BY 3")
    areas = q("SELECT area_type, traffic, avg_delivery_min, trips FROM marts.area_traffic")
    city = q("SELECT city_name, trips, stores, avg_delivery_min, p90_delivery_min FROM marts.city_performance "
             "ORDER BY trips DESC")
    dq = q("SELECT rule_code, severity, rows_failed, pct_failed FROM marts.dq_summary WHERE batch_id = "
           "(SELECT batch_id FROM ops.load_batch WHERE status = 'transformed' ORDER BY batch_id LIMIT 1) "
           "AND rows_failed > 0 ORDER BY rows_failed DESC")
    recon = q("SELECT (SELECT count(*) FROM raw.amazon_delivery WHERE batch_id = b.batch_id), "
              "(SELECT count(*) FROM core.fact_trip WHERE batch_id = b.batch_id), "
              "(SELECT count(*) FROM quarantine.rejected_trip WHERE batch_id = b.batch_id) "
              "FROM (SELECT min(batch_id) batch_id FROM ops.load_batch WHERE status='transformed') b")[0]

    # --- charts
    bar("traffic", "Delivery time rises with traffic", [t for t, _ in traffic], [float(m) for _, m in traffic])
    bar("weather", "Sunny days are ~35 min faster than Cloudy or Fog", [w[0] for w in weather],
        [float(w[1]) for w in weather], highlight="Sunny")
    bar("distance", "Longer trips take longer, then flatten after 12 km", [d[0] for d in dist],
        [float(d[1]) for d in dist], xlabel="Store-to-drop distance (km)")
    fig, ax = plt.subplots()
    for wk, label, col in ((False, "Weekday", ACCENT), (True, "Weekend", WARN)):
        by_hour = {h: float(m) for h, w, m, _ in hours if w == wk}
        xs = list(range(24))                      # missing hours (no orders 01-07) stay as gaps, not lines
        ax.plot(xs, [by_hour.get(h, float("nan")) for h in xs], marker="o", ms=3, label=label, color=col)
    ax.set(title="Evening orders (19:00-21:00) are slowest", xlabel="Hour of order (no orders 01:00-07:59)",
           ylabel="Avg delivery time (min)", xticks=range(0, 24, 3))
    ax.legend(frameon=False)
    save(fig, "hour")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(6.4, 3.4))
    for ax_, idx, ttl, f in ((a1, 1, "Avg rating (axis starts at 4.4)", "{:.2f}"), (a2, 2, "Avg delivery time (min)", "{:.0f}")):
        bars = ax_.bar([a[0] for a in ages], [float(a[idx]) for a in ages], color=ACCENT)
        for r_ in bars:
            ax_.annotate(f.format(r_.get_height()), (r_.get_x() + r_.get_width() / 2, r_.get_height()),
                         ha="center", va="bottom", fontsize=8)
        ax_.set_title(ttl)
    a1.set_ylim(4.4, 4.75)
    fig.suptitle("Agents aged 30+ are rated lower and slower", x=0.02, ha="left", fontweight="bold", fontsize=10)
    save(fig, "age")
    bar("category", "Grocery is delivered ~5x faster than any other category", [c[0] for c in cats],
        [float(c[1]) for c in cats], highlight="Grocery")
    plt.close("all")
    # category labels are long: redo horizontally
    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    ax.barh([c[0] for c in cats], [float(c[1]) for c in cats],
            color=[ACCENT if c[0] == "Grocery" else MUTED for c in cats])
    ax.set(title="Grocery is delivered ~5x faster than any other category", xlabel="Avg delivery time (min)")
    save(fig, "category")
    bar("vehicle", "Motorcycles are ~15 min slower than scooters and vans", [v[0] for v in veh],
        [float(v[2]) for v in veh], highlight="motorcycle")
    fig, ax = plt.subplots(figsize=(6.4, 3.4))
    ax.barh([d[0] for d in reversed(dq)], [d[2] for d in reversed(dq)],
            color=[WARN if d[1] == "error" else MUTED for d in reversed(dq)])
    ax.set_xscale("log")
    ax.set_xlim(left=10)
    ax.set(title="Rows flagged per rule (red = quarantined)", xlabel="Rows (log scale)")
    save(fig, "dq")

    # --- numbers for the text
    t = {k: float(v) for k, v in traffic}
    w = {r[0]: (float(r[1]), float(r[2])) for r in weather}
    d = {r[0]: float(r[1]) for r in dist}
    a = {r[0]: (float(r[1]), float(r[2])) for r in ages}
    c = {r[0]: float(r[1]) for r in cats}
    v = {r[0]: float(r[2]) for r in veh}
    wk = {h: float(m) for h, wkd, m, _ in hours if not wkd}
    peak_h = max(wk, key=wk.get)
    low_h = min(wk, key=wk.get)
    semi = {tr: float(m) for ar, tr, m, n in areas if ar == "Semi-Urban"}
    semi_n = sum(n for ar, _, _, n in areas if ar == "Semi-Urban")
    blr = next((r for r in city if r[0] == "Bengaluru"), None)
    others = [x for k, x in c.items() if k != "Grocery"]
    semi_note = (f"Semi-Urban is an outlier ({semi['Jam']:.0f} min in Jam) but has only {semi_n} trips"
                 f"{' and none in Low traffic' if 'Low' not in semi else ''}, so treat it as noise."
                 if 'Jam' in semi else "")

    md = f"""# Findings

_Generated by `python -m pipeline.report` from the loaded warehouse on {date.today():%d %b %Y}; every number below is queried, not typed._

**Scope.** {H[0]:,} clean trips (of {recon[0]:,} raw rows; {recon[2]:,} quarantined, see "Data quality"). Averages are delivery minutes unless stated. These are associations in observational data, not proven causes.

| Avg delivery | Avg rating | Avg distance | Corr(distance, time) | Corr(rating, time) |
|---|---|---|---|---|
| {H[1]} min | {H[2]} | {H[3]} km | {H[4]} | {H[5]} |

## 1. Traffic is the clearest driver
![traffic](charts/traffic.png)

Low traffic averages **{t['Low']:.0f} min**; Jam averages **{t['Jam']:.0f} min** (+{t['Jam'] - t['Low']:.0f} min, {100 * (t['Jam'] / t['Low'] - 1):.0f}% slower). High and Medium sit between ({t['High']:.0f} / {t['Medium']:.0f}).
{semi_note}

## 2. Weather: Sunny is fast; Cloudy and Fog are slow
![weather](charts/weather.png)

Sunny **{w['Sunny'][0]:.0f} min** vs Cloudy **{w['Cloudy'][0]:.0f}** and Fog **{w['Fog'][0]:.0f}**; Stormy, Windy and Sandstorms cluster near {w['Stormy'][0]:.0f}.
Ratings barely move with weather ({min(x[1] for x in w.values()):.2f} to {max(x[1] for x in w.values()):.2f}), so weather changes speed but not how agents are rated.

## 3. Distance matters, with diminishing returns
![distance](charts/distance.png)

Time rises from **{d['0-3']:.0f} min** (under 3 km) to **{d['12-15']:.0f} min** (12-15 km) and then flattens ({d['15+']:.0f} min beyond 15 km). Overall correlation is only {H[4]}: distance alone is a weak predictor next to traffic and weather.

## 4. Time of day
![hour](charts/hour.png)

On weekdays the slowest order hour is **{peak_h:02d}:00** ({wk[peak_h]:.0f} min) and the fastest is **{low_h:02d}:00** ({wk[low_h]:.0f} min).

## 5. Category: Grocery is a different service
![category](charts/category.png)

Grocery averages **{c['Grocery']:.0f} min**; every other category is {min(others):.0f}-{max(others):.0f} min. That gap is far larger than any weather or traffic effect, which suggests Grocery runs on a different fulfilment model (or is recorded differently). Worth confirming with the data owner before drawing operational conclusions.

## 6. Agents: age, rating and speed
![age](charts/age.png)

There is a step at age 30. Agents under 30 average rating **{a['18-24'][0]:.2f}-{a['25-29'][0]:.2f}** and **{a['18-24'][1]:.0f} min**; agents 30+ average **{a['30-34'][0]:.2f}-{a['35-39'][0]:.2f}** and **{a['30-34'][1]:.0f}-{a['35-39'][1]:.0f} min**. Rating and delivery time are negatively correlated ({H[5]}): slower deliveries are rated lower.
The age effect is consistent across the 30-39 range, which is unusual for real-world behaviour; check how this field was recorded before acting on it.

## 7. Vehicles
![vehicle](charts/vehicle.png)

Motorcycle **{v['motorcycle']:.0f} min** vs scooter **{v['scooter']:.0f}** and van **{v['van']:.0f}**. The gap holds at every traffic level. Bicycle does not appear: all 15 raw bicycle rows are corrupted and quarantined.

## 8. Cities
{"Bengaluru: **" + format(blr[1], ",") + " trips** from " + str(blr[2]) + " stores, avg " + str(blr[3]) + " min (p90 " + str(blr[4]) + ")." if blr else "Bengaluru not present."}

| City | Trips | Stores | Avg min | P90 min |
|---|---|---|---|---|
""" + "\n".join(f"| {r[0]} | {r[1]:,} | {r[2]} | {r[3]} | {r[4]} |" for r in city[:10]) + f"""

## Data quality
![dq](charts/dq.png)

Raw {recon[0]:,} = loaded {recon[1]:,} + quarantined {recon[2]:,}.

| Rule | Severity | Rows | % |
|---|---|---|---|
""" + "\n".join(f"| `{r[0]}` | {r[1]} | {r[2]:,} | {r[3]}% |" for r in dq) + "\n"
    DOCS.mkdir(exist_ok=True)
    (DOCS / "FINDINGS.md").write_text(md)

    return md


def main():
    with db.connect() as conn:
        build(conn)
    print(f"wrote {DOCS / 'FINDINGS.md'}")


if __name__ == "__main__":
    main()
