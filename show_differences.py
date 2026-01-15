#!/usr/bin/env python3
"""
Show examples of data differences between optimized and non-optimized queries
"""

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load private key
with open('/Users/anas/snowflake_key.pem', 'rb') as key_file:
    private_key_data = key_file.read()

private_key = serialization.load_pem_private_key(
    private_key_data,
    password=None,
    backend=default_backend()
)

private_key_pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account='BZB11272',
    user='SERVICE_USER',
    warehouse='SA_WH',
    database='CISCO_DEMO',
    schema='CISCO',
    role='ACCOUNTADMIN',
    private_key=private_key_pkb
)

cursor = conn.cursor()

# Load and execute the optimized query
with open('tpch_snowflake_queries/query_4_dev_optimized.sql', 'r') as f:
    optimized_sql = f.read()

# Load and execute the non-optimized query
with open('tpch_snowflake_queries/query_4_dev.sql', 'r') as f:
    non_optimized_sql = f.read()

# Disable cache
cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")

# Execute optimized query
cursor.execute(optimized_sql)
optimized_data = cursor.fetchall()
optimized_cols = [desc[0] for desc in cursor.description]

# Execute non-optimized query
cursor.execute(non_optimized_sql)
non_optimized_data = cursor.fetchall()
non_optimized_cols = [desc[0] for desc in cursor.description]

# Compare first 5 rows that differ
diff_count = 0
print("=" * 120)
print("EXAMPLES OF DATA VALIDATION DIFFERENCES")
print("=" * 120)
print(f"\nColumns: {optimized_cols}\n")

for i, (opt_row, non_opt_row) in enumerate(zip(optimized_data, non_optimized_data)):
    if opt_row != non_opt_row:
        diff_count += 1
        if diff_count <= 5:  # Show first 5 differences
            print(f"\nRow {i+1} - Difference Found:")
            print("-" * 120)
            print(f"Optimized:     {opt_row}")
            print(f"Non-Optimized: {non_opt_row}")
            print("\nColumn-level differences:")
            for j, (opt_val, non_opt_val) in enumerate(zip(opt_row, non_opt_row)):
                if opt_val != non_opt_val:
                    print(f"  • {optimized_cols[j]}: '{opt_val}' vs '{non_opt_val}'")

print(f"\n{'=' * 120}")
print(f"Summary: {diff_count} total rows with differences (out of 10,000)")
print("=" * 120)

cursor.close()
conn.close()
