from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

# -----------------------------------------
# CONFIG — مهم جداً لبايثون أني وير
# -----------------------------------------

# Get BASE DIR = نفس فولدر server.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# كامل المسار الصحيح للـ DB
DB_PATH = os.path.join(BASE_DIR, "datawarehouse.db")

app = Flask(__name__)
CORS(app)

# -----------------------------------------
# HELPER — Query SQLite safely
# -----------------------------------------
def query_db(query, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print("Database Error:", e)
        return []

# -----------------------------------------
# ROOT — Test endpoint
# -----------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "API working",
        "database": DB_PATH
    })

# -----------------------------------------
# ENDPOINT — List Systems
# -----------------------------------------
@app.route("/systems")
def systems():
    q = "SELECT system_name FROM dim_system"
    return jsonify(query_db(q))

# -----------------------------------------
# ENDPOINT — List Departments
# -----------------------------------------
@app.route("/departments")
def departments():
    q = "SELECT department_name FROM dim_department"
    return jsonify(query_db(q))

# -----------------------------------------
# ENDPOINT — Filter Utilization
# -----------------------------------------
@app.route("/utilization/filter")
def filter_data():

    system_name = request.args.get("system")
    dept_name = request.args.get("department")

    sort_by = request.args.get("sort_by", "usage_date")
    sort_order = request.args.get("sort_order", "DESC")

    valid_cols = {
        "system_name": "s.system_name",
        "department_name": "d.department_name",
        "utilization_pct": "f.utilization_pct",
        "usage_date": "f.usage_date",
        "usage_time": "f.usage_time"
    }
    valid_order = ["ASC", "DESC"]

    sort_col = valid_cols.get(sort_by, "f.usage_date")
    sort_ord = sort_order if sort_order in valid_order else "DESC"

    query = """
        SELECT 
            f.id,
            s.system_name,
            d.department_name,
            f.utilization_pct,
            f.usage_date,
            f.usage_time
        FROM fact_utilization f
        JOIN dim_system s ON f.system_id = s.system_id
        JOIN dim_department d ON f.dept_id = d.dept_id
        WHERE 1=1
    """

    params = []

    if system_name and system_name != "null":
        query += " AND s.system_name = ?"
        params.append(system_name)

    if dept_name and dept_name != "null":
        query += " AND d.department_name = ?"
        params.append(dept_name)

    query += f" ORDER BY {sort_col} {sort_ord}, f.usage_time DESC"

    return jsonify(query_db(query, params))

# -----------------------------------------
# RUN (for local testing ONLY)
# PythonAnywhere uses WSGI, not this block
# -----------------------------------------
if __name__ == "__main__":
    print("Running local Flask server...")
    app.run(debug=True, port=5000)
