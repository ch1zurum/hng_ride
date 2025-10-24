import sqlite3, pandas as pd
conn = sqlite3.connect("hng_ride.db")
rides = pd.read_csv("rides_raw.csv")
rides.to_sql("rides_raw", conn, index=False, if_exists="replace")
# repeat for riders_raw, payments_raw, drivers_raw
riders = pd.read_csv("riders_raw.csv")
riders.to_sql("riders_raw", conn, index=False, if_exists="replace")
payments = pd.read_csv("payments_raw.csv")
payments.to_sql("payments_raw", conn, index=False, if_exists="replace")
drivers = pd.read_csv("drivers_raw.csv")
drivers.to_sql("drivers_raw", conn, index=False, if_exists="replace")

print("✅ CSVs loaded into hng_ride.db")

def show(title, query):
    print(f"\n{'='*70}\n{title}\n{'='*70}")
    df = pd.read_sql(query, conn)
    print(df.head(10))
    print(f"\n→ {len(df)} rows returned.")
    return df

#3. BUSINESS QUESTIONS ---

# Q1: Top 10 longest rides
q1 = """
SELECT r.ride_id,
       r.distance_km,
       d.name  AS driver_name,
       ri.name AS rider_name,
       r.pickup_city,
       r.dropoff_city,
       p.method AS payment_method
FROM rides_raw r
LEFT JOIN drivers_raw d ON d.driver_id = r.driver_id
LEFT JOIN riders_raw  ri ON ri.rider_id = r.rider_id
LEFT JOIN payments_raw p ON p.ride_id = r.ride_id
WHERE p.amount > 0
ORDER BY r.distance_km DESC
LIMIT 10;
"""

# Q2: Riders who signed up in 2021 and still rode in 2024
q2 = """
SELECT COUNT(DISTINCT r1.rider_id) AS riders_2021_still_active_2024
FROM riders_raw r1
JOIN rides_raw r2 ON r1.rider_id = r2.rider_id
WHERE CAST(substr(r1.signup_date,1,4) AS INT) = 2021
  AND substr(r2.pickup_time,1,4) = '2024';
"""

# Q3: Quarterly revenue comparison (2021–2024)
q3 = """
SELECT
  substr(p.paid_date,1,4) AS year,
  CASE
    WHEN substr(p.paid_date,6,2) IN ('01','02','03') THEN 'Q1'
    WHEN substr(p.paid_date,6,2) IN ('04','05','06') THEN 'Q2'
    WHEN substr(p.paid_date,6,2) IN ('07','08','09') THEN 'Q3'
    ELSE 'Q4'
  END AS quarter,
  SUM(p.amount) AS revenue
FROM payments_raw p
WHERE p.amount > 0
GROUP BY year, quarter
ORDER BY year, quarter;
"""

# Q4: Average monthly rides per driver (consistency)
q4 = """
WITH monthly AS (
  SELECT driver_id,
         substr(pickup_time,1,7) AS month,
         COUNT(DISTINCT ride_id) AS rides_in_month
  FROM rides_raw
  WHERE pickup_time BETWEEN '2021-06-01' AND '2024-12-31'
  GROUP BY driver_id, month
)
SELECT d.name AS driver_name,
       SUM(m.rides_in_month) AS total_rides,
       COUNT(m.month) AS active_months,
       ROUND(SUM(m.rides_in_month)*1.0/COUNT(m.month),2) AS avg_monthly_rides
FROM monthly m
LEFT JOIN drivers_raw d ON d.driver_id = m.driver_id
GROUP BY m.driver_id
ORDER BY avg_monthly_rides DESC
LIMIT 5;
"""

# Q5: Cancellation rate per city
q5 = """
WITH requests AS (
  SELECT pickup_city, COUNT(DISTINCT ride_id) AS requests
  FROM rides_raw
  GROUP BY pickup_city
),
cancels AS (
  SELECT pickup_city, COUNT(DISTINCT ride_id) AS cancelled
  FROM rides_raw
  WHERE LOWER(status)='cancelled'
  GROUP BY pickup_city
)
SELECT r.pickup_city,
       COALESCE(c.cancelled,0) AS cancelled,
       r.requests,
       ROUND(COALESCE(c.cancelled,0)*1.0 / r.requests,3) AS cancellation_rate
FROM requests r
LEFT JOIN cancels c ON r.pickup_city=c.pickup_city
ORDER BY cancellation_rate DESC;
"""

# Q6: Riders with >10 rides who never paid cash
q6 = """
WITH rides_count AS (
  SELECT rider_id, COUNT(DISTINCT ride_id) AS rides_count
  FROM rides_raw
  GROUP BY rider_id
),
cash_users AS (
  SELECT DISTINCT r.rider_id
  FROM rides_raw r
  JOIN payments_raw p ON p.ride_id=r.ride_id
  WHERE LOWER(p.method)='cash'
)
SELECT ri.name AS rider_name, rc.rides_count
FROM rides_count rc
JOIN riders_raw ri ON rc.rider_id=ri.rider_id
LEFT JOIN cash_users cu ON rc.rider_id=cu.rider_id
WHERE rc.rides_count>10 AND cu.rider_id IS NULL
ORDER BY rc.rides_count DESC;
"""

# Q7: Top 3 drivers in each city by total revenue
q7 = """
WITH rev AS (
  SELECT r.pickup_city, r.driver_id, SUM(p.amount) AS revenue
  FROM rides_raw r
  JOIN payments_raw p ON r.ride_id=p.ride_id
  WHERE p.amount>0
  GROUP BY r.pickup_city, r.driver_id
)
SELECT pickup_city, driver_id, d.name AS driver_name, revenue
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY pickup_city ORDER BY revenue DESC) AS rnk
  FROM rev
)
JOIN drivers_raw d ON d.driver_id=driver_id
WHERE rnk<=3
ORDER BY pickup_city, rnk;
"""

# Q8: Bonus-qualified drivers (≥30 rides, rating≥4.5, cancel<5%)
q8 = """
WITH completed AS (
  SELECT driver_id, COUNT(DISTINCT ride_id) AS completed_rides
  FROM rides_raw
  WHERE ride_id IN (SELECT ride_id FROM payments_raw WHERE amount>0)
  GROUP BY driver_id
),
cancelled AS (
  SELECT driver_id,
         COUNT(CASE WHEN LOWER(status)='cancelled' THEN 1 END) AS cancelled,
         COUNT(*) AS total
  FROM rides_raw
  GROUP BY driver_id
)
SELECT d.name AS driver_name,
       c.completed_rides,
       d.rating,
       ROUND(coalesce(can.cancelled,0)*1.0/coalesce(can.total,1),3) AS cancel_rate
FROM completed c
JOIN drivers_raw d ON d.driver_id=c.driver_id
LEFT JOIN cancelled can ON can.driver_id=c.driver_id
WHERE c.completed_rides>=30
  AND d.rating>=4.5
  AND ROUND(coalesce(can.cancelled,0)*1.0/coalesce(can.total,1),3)<0.05
ORDER BY c.completed_rides DESC, d.rating DESC
LIMIT 10;
"""

# --- 4. RUN ALL QUERIES & DISPLAY RESULTS ---
show("Q1: Top 10 Longest Rides", q1)
show("Q2: Riders Who Signed Up in 2021 and Still Rode in 2024", q2)
show("Q3: Quarterly Revenue by Year", q3)
show("Q4: Average Monthly Rides per Driver", q4)
show("Q5: Cancellation Rate per City", q5)
show("Q6: Riders with >10 Rides Who Never Paid Cash", q6)
show("Q7: Top 3 Drivers in Each City by Revenue", q7)
show("Q8: Bonus-Qualified Drivers", q8)

# 4. EXECUTE & SAVE ALL RESULTS
# -------------------------------
all_results = {}
for title, q in queries.items():
    df = run_query(title, q)
    all_results[title] = df

# Save all results to one Excel file
excel_file = "HNG_Ride_Analysis.xlsx"
with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
    for title, df in all_results.items():
        df.to_excel(writer, sheet_name=title[:31], index=False)

print(f"\n✅ All results saved to {excel_file}")
# Close DB connection
conn.close()
print("\n✅ All 8 analyses complete. Results displayed above.")