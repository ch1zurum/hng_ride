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
print("\nTop 10 longest rides:")
print(pd.read_sql(q1, conn))

# 4. Example query: Riders who signed up in 2021 and still rode in 2024
q2 = """
SELECT COUNT(DISTINCT r1.rider_id) AS riders_2021_still_active_2024
FROM riders_raw r1
JOIN rides_raw r2
  ON r1.rider_id = r2.rider_id
WHERE CAST(substr(r1.signup_date,1,4) AS INT) = 2021
  AND substr(r2.pickup_time,1,4) = '2024';
"""
print("\nActive riders from 2021 to 2024:")
print(pd.read_sql(q2, conn))

conn.close()