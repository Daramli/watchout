from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

app = Flask(__name__)
CORS(app)

# ====================================================
# Database Path (Important for Render)
# ====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "datawarehouse.db")

# ====================================================
# Helper function
# ====================================================
def query_db(query, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Database Error: {e}")
        return []

# ====================================================
# Root Endpoint (Health Check)
# ====================================================
@app.route("/")
def home():
    return jsonify({"status": "API is working", "db_path": DB_PATH})

# ====================================================
# Systems Endpoint
# ====================================================
@app.route("/systems", methods=["GET"])
def get_systems():
    data = query_db("SELECT system_name FROM dim_system")
    return jsonify(data)

# ====================================================
# Departments Endpoint
# ====================================================
@app.route("/departments", methods=["GET"])
def get_departments():
    data = query_db("SELECT department_name FROM dim_department")
    return jsonify(data)

# ====================================================
# Filter Endpoint
# ====================================================
@app.route("/utilization/filter", methods=["GET"])
def filter_utilization():
    system_name = request.args.get("system")
    dept_name = request.args.get("department")

    # Sorting parameters
    sort_by = request.args.get("sort_by", "usage_date")
    sort_order = request.args.get("sort_order", "DESC")

    valid_columns = {
        "system_name": "s.system_name",
        "department_name": "d.department_name",
        "utilization_pct": "f.utilization_pct",
        "usage_date": "f.usage_date",
        "usage_time": "f.usage_time",
    }
    valid_orders = ["ASC", "DESC"]

    sql_sort_column = valid_columns.get(sort_by, "f.usage_date")
    sql_sort_order = sort_order if sort_order in valid_orders else "DESC"

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

    if system_name and system_name != "null" and system_name != "":
        query += " AND s.system_name = ?"
        params.append(system_name)

    if dept_name and dept_name != "null" and dept_name != "":
        query += " AND d.department_name = ?"
        params.append(dept_name)

    query += f" ORDER BY {sql_sort_column} {sql_sort_order}"

    if sql_sort_column not in ["f.usage_date", "f.usage_time"]:
        query += ", f.usage_date DESC, f.usage_time DESC"

    data = query_db(query, params)
    return jsonify(data)

# ====================================================
# Run locally (Render will NOT use this block)
# ====================================================
if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))
    print(f"Server running on port {PORT}")
    app.run(debug=False, host="0.0.0.0", port=PORT)
