import pandas as pd
import sqlite3
import os

# ============================
# 1) Paths
# ============================

CSV_PATH = "data.txt" 
DB_PATH = "datawarehouse.db" 
CLEANED_CSV = "cleaned_data.csv" 

# ============================
# 2) Read CSV
# ============================

print("📥 Reading raw file...")
df = pd.read_csv(CSV_PATH)

# تنظيف عناوين الأعمدة
df.columns = df.columns.str.strip()

# ============================
# 3) Clean + Split Timestamp
# ============================

print("⏱️ Splitting timestamp into date/time...")

df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df = df.dropna(subset=['timestamp']).copy()

df['usage_date'] = df['timestamp'].dt.date.astype(str)
df['usage_time'] = df['timestamp'].dt.time.astype(str)
df['year'] = df['timestamp'].dt.year
df['month'] = df['timestamp'].dt.month
df['day'] = df['timestamp'].dt.day
df['hour'] = df['timestamp'].dt.hour

# ============================
# 4) Save cleaned data (Optional)
# ============================

df.to_csv(CLEANED_CSV, index=False)
print(f"💾 Cleaned file saved as: {CLEANED_CSV}")

# ============================
# 5) Create SQLite Warehouse (تم التعديل هنا)
# ============================

print("🗄️ Creating SQLite Data Warehouse...")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.executescript("""
PRAGMA foreign_keys = ON;

-- 🚨 FIX: مسح الجداول القديمة المكررة
DROP TABLE IF EXISTS fact_utilization;
DROP TABLE IF EXISTS dim_system;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE IF NOT EXISTS dim_system (
    system_id INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_department (
    dept_id INTEGER PRIMARY KEY AUTOINCREMENT,
    department_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY AUTOINCREMENT,
    usage_date TEXT UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER
);

CREATE TABLE IF NOT EXISTS fact_utilization (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key INTEGER NOT NULL,
    dept_id INTEGER NOT NULL,
    system_id INTEGER NOT NULL,
    utilization_pct REAL,
    usage_date TEXT,
    usage_time TEXT,
    -- 🚨 FIX: قيد التفرد لمنع التكرار في المستقبل
    UNIQUE (date_key, dept_id, system_id, usage_time), 
    FOREIGN KEY(date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY(dept_id) REFERENCES dim_department(dept_id),
    FOREIGN KEY(system_id) REFERENCES dim_system(system_id)
);
""")

conn.commit()

# ============================
# 6) Helper functions (بدون تغيير)
# ============================

def get_or_create_system(name):
    cur.execute("INSERT OR IGNORE INTO dim_system(system_name) VALUES (?)", (name,))
    conn.commit()
    cur.execute("SELECT system_id FROM dim_system WHERE system_name=?", (name,))
    return cur.fetchone()[0]

def get_or_create_department(name):
    cur.execute("INSERT OR IGNORE INTO dim_department(department_name) VALUES (?)", (name,))
    conn.commit()
    cur.execute("SELECT dept_id FROM dim_department WHERE department_name=?", (name,))
    return cur.fetchone()[0]

def get_or_create_date(d, year, month, day, hour):
    cur.execute("""
        INSERT OR IGNORE INTO dim_date(usage_date, year, month, day, hour)
        VALUES (?, ?, ?, ?, ?)
    """, (d, year, month, day, hour))
    conn.commit()
    cur.execute("SELECT date_key FROM dim_date WHERE usage_date=?", (d,))
    return cur.fetchone()[0]

# ============================
# 7) Insert Dimensions (بدون تغيير)
# ============================

print("🏗️ Building Dimensions...")

for s in df['system'].unique():
    get_or_create_system(s)

for dep in df['department'].unique():
    get_or_create_department(dep)

date_records = df[['usage_date','year','month','day','hour']].drop_duplicates()

for _, row in date_records.iterrows():
    get_or_create_date(row['usage_date'], row['year'], row['month'], row['day'], row['hour'])

# ============================
# 8) Insert Fact Table (بدون تغيير)
# ============================

print("📥 Inserting Fact rows...")

fact_rows = []

for _, row in df.iterrows():

    system_id = get_or_create_system(row['system'])
    dept_id = get_or_create_department(row['department'])
    date_key = get_or_create_date(row['usage_date'], row['year'], row['month'], row['day'], row['hour'])

    util = None
    for col in df.columns:
        if "util" in col.lower():
            util = float(row[col])
            break

    fact_rows.append((date_key, dept_id, system_id, util, row['usage_date'], row['usage_time']))

cur.executemany("""
INSERT OR IGNORE INTO fact_utilization(date_key, dept_id, system_id, utilization_pct, usage_date, usage_time)
VALUES (?, ?, ?, ?, ?, ?)
""", fact_rows)

conn.commit()

# ============================
# 9) Summary
# ============================

cur.execute("SELECT COUNT(*) FROM fact_utilization")
print("✅ Fact rows inserted:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM dim_system")
print("   Systems:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM dim_department")
print("   Departments:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM dim_date")
print("   Dates:", cur.fetchone()[0])

conn.close()

print("\n🎉 ETL Completed Successfully!")
print(f"📦 Data Warehouse file: {DB_PATH}")