#!/usr/bin/env python3
"""
DBT Model Performance and Correctness Comparison Script

IMPORTANT: HOW TO USE THIS SCRIPT
================================================================================

This script compares two DBT models (e.g., optimized vs non-optimized) by:
1. **Compiling the models**: Uses `dbt compile` to generate compiled SQL from
   your DBT model files (handles Jinja templating, refs, sources, etc.)
2. **Executing queries**: Runs both compiled SQL queries on Snowflake with
   cache disabled to get accurate performance metrics
3. **Validating correctness**: Compares results row-by-row to ensure both
   queries return identical data
4. **Measuring performance**: Collects execution time, bytes scanned, and other
   Snowflake metrics
5. **Outputting results**: Displays comparison in console and saves to JSON

COMPILATION WORKFLOW:
---------------------
The script automatically handles the compilation of both models specified in
MODEL_1_PATH and MODEL_2_PATH configuration variables. For each model:

1. Runs: `dbt compile --select <model_path>`
   - Example: `dbt compile --select altimate/development`
   - This compiles the .sql file from models/ directory

2. Reads compiled SQL from: `target/compiled/<project_name>/models/<model_path>.sql`
   - Example: `target/compiled/jaffle_shop_test/models/altimate/development.sql`
   - The compiled SQL has all Jinja logic resolved and is ready to execute

3. Uses the compiled SQL for execution and validation
   - Ensures you're testing the actual SQL that DBT would run
   - No need for manual compilation or SQL copying

CONFIGURATION:
--------------
Before running this script, configure the following variables:

1. SNOWFLAKE_CONFIG: Your Snowflake connection details
   - account, user, warehouse, database, schema, role

2. Authentication (choose one):
   - SNOWFLAKE_PASSWORD: For password authentication (simpler)
   - PRIVATE_KEY_PATH: For key-pair authentication (more secure)

3. DBT_PROJECT_DIR: Absolute path to your DBT project directory
   - Example: "/Users/anas/demos/cisco_demo/jaffle-shop"

4. DBT_PROFILES_DIR: Path to your DBT profiles directory
   - Default: "~/.dbt" (where profiles.yml is located)

5. MODEL_1_PATH and MODEL_2_PATH: Relative paths to models from models/ directory
   - Example: "altimate/development" → models/altimate/development.sql
   - Example: "cisco/query2_sales_revenue" → models/cisco/query2_sales_revenue.sql

USAGE:
------
1. Ensure DBT is installed and profiles configured:
   ```bash
   dbt --version
   dbt debug  # Verify connection works
   ```

2. Set the model paths you want to compare (edit this script):
   ```python
   MODEL_1_PATH = "cisco/query2_sales_revenue"
   MODEL_2_PATH = "cisco/query2_sales_revenue_optimized"
   ```

3. Run the script:
   ```bash
   python dbt_model_compare.py
   ```

4. Review output:
   - Console shows step-by-step progress
   - JSON file contains detailed metrics

WHAT GETS COMPILED:
-------------------
The script compiles your raw DBT model files which may contain:
- Jinja templating: {{ ref('other_model') }}, {{ source('schema', 'table') }}
- Macros: {{ macro_name() }}
- DBT functions: {{ config(...) }}
- Variables: {{ var('variable_name') }}

Example model file (models/altimate/development.sql):
```sql
{{ config(materialized='view') }}

SELECT
    c_custkey,
    c_name,
    COUNT(o_orderkey) as order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ source('tpch', 'orders') }} o
    ON c.c_custkey = o.o_custkey
GROUP BY c_custkey, c_name
```

After compilation (target/compiled/.../development.sql):
```sql
SELECT
    c_custkey,
    c_name,
    COUNT(o_orderkey) as order_count
FROM cisco_demo.cisco.stg_customers c
LEFT JOIN cisco_demo.tpch.orders o
    ON c.c_custkey = o.o_custkey
GROUP BY c_custkey, c_name
```

The script uses the compiled version for execution and validation.

VALIDATION DETAILS:
-------------------
The script performs comprehensive validation to ensure both models produce
identical results:

1. Row Count Check: Ensures both queries return the same number of rows
2. Column Check: Verifies column names and order match exactly
3. Value Check: Compares all values (order-independent) to detect differences

If validation fails, the script reports exactly what differs.

OUTPUT:
-------
- Console: Detailed step-by-step progress with metrics
- JSON file (dbt_model_comparison_results.json):
  * Query IDs for both models
  * Execution times (Python and Snowflake)
  * Bytes scanned
  * Validation results
  * All performance metrics

REQUIREMENTS:
-------------
Python packages:
- snowflake-connector-python
- pandas
- cryptography (for key-pair auth)
- dbt-core and dbt-snowflake

Install with:
```bash
pip install snowflake-connector-python pandas cryptography dbt-core dbt-snowflake
```

Dependencies:
- snowflake-connector-python
- pandas
- cryptography (for key-pair authentication)
- dbt-core (for compiling models)
"""

import os
import sys
import time
import json
import subprocess
import tempfile
import snowflake.connector
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Snowflake configuration - update with your account details
SNOWFLAKE_CONFIG = {
    'account': 'BZB11272',
    'user': 'SERVICE_USER',
    'warehouse': 'SA_WH',
    'database': 'CISCO_DEMO',
    'schema': 'CISCO',
    'role': 'ACCOUNTADMIN'
}

# Authentication options (configure ONE of these):
# Option 1: Password authentication (simplest)
SNOWFLAKE_PASSWORD = None  # Example: 'your-password-here'

# Option 2: Key-pair authentication (more secure)
PRIVATE_KEY_PATH = '/Users/anas/snowflake_key.pem'

# DBT project configuration
DBT_PROJECT_DIR = "/Users/anas/demos/cisco_demo/jaffle-shop"
DBT_PROFILES_DIR = os.path.expanduser("~/.dbt")

# Models to compare (relative paths from models/ directory)
# Examples:
#   "cisco/query1_service_quotes"
#   "cisco/query2_sales_revenue"
#   "cisco/query2_sales_revenue_optimized"
MODEL_1_PATH = "altimate/query_4"  # Non-optimized model
MODEL_2_PATH = "altimate/query_4_optimized"  # Optimized model

# Output file for results
OUTPUT_JSON_PATH = "dbt_model_comparison_results.json"

# SQL commands
DISABLE_CACHE_SQL = "ALTER SESSION SET USE_CACHED_RESULT = FALSE;"
GET_LAST_QUERY_ID_SQL = "SELECT LAST_QUERY_ID();"

GET_QUERY_METRICS_SQL = """
SELECT 
    query_id,
    execution_time,
    total_elapsed_time,
    bytes_scanned,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
WHERE query_id = %s
"""


# ============================================================================
# DBT OPERATIONS
# ============================================================================

def compile_dbt_model(model_path: str, project_dir: str = DBT_PROJECT_DIR) -> str:
    """
    Compiles a DBT model to SQL using 'dbt compile'.
    
    Args:
        model_path: Path to the DBT model (e.g., "cisco/query1_service_quotes")
        project_dir: Path to the DBT project directory
    
    Returns:
        str: Compiled SQL content
    
    Raises:
        RuntimeError: If DBT compilation fails
        FileNotFoundError: If compiled SQL file is not found
    """
    print(f"\nCompiling DBT model: {model_path}")
    print("-" * 80)
    
    # Change to project directory
    original_dir = os.getcwd()
    os.chdir(project_dir)
    
    try:
        # Run dbt compile for the specific model
        cmd = [
            "dbt", "compile",
            "--select", model_path,
            "--profiles-dir", DBT_PROFILES_DIR
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            print(f"✗ DBT compilation failed!")
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
            raise RuntimeError(f"DBT compilation failed for model: {model_path}")
        
        print("✓ DBT compilation successful")
        
        # Find the compiled SQL file
        # DBT compiles models to target/compiled/{project_name}/models/{model_path}.sql
        project_name = "jaffle_shop_test"  # From dbt_project.yml
        compiled_path = os.path.join(
            project_dir,
            "target",
            "compiled",
            project_name,
            "models",
            f"{model_path}.sql"
        )
        
        print(f"Looking for compiled SQL at: {compiled_path}")
        
        if not os.path.exists(compiled_path):
            raise FileNotFoundError(f"Compiled SQL not found at: {compiled_path}")
        
        # Read the compiled SQL
        with open(compiled_path, 'r') as f:
            compiled_sql = f.read().strip()
        
        print(f"✓ Loaded compiled SQL ({len(compiled_sql)} characters)")
        
        return compiled_sql
    
    finally:
        # Return to original directory
        os.chdir(original_dir)


def get_dbt_model_name(model_path: str) -> str:
    """
    Extracts the model name from a model path.
    
    Args:
        model_path: Model path (e.g., "cisco/query1_service_quotes")
    
    Returns:
        str: Model name (e.g., "query1_service_quotes")
    """
    return Path(model_path).name


# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================

def load_private_key(key_path: str) -> bytes:
    """
    Loads and parses a private key from a PEM file for Snowflake key-pair authentication.
    
    Args:
        key_path: Path to the private key PEM file
    
    Returns:
        bytes: Parsed private key in DER format
    
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key file is invalid or requires a passphrase
    """
    print(f"Loading private key from: {key_path}")
    
    with open(key_path, 'rb') as key_file:
        private_key_data = key_file.read()
    
    private_key = serialization.load_pem_private_key(
        private_key_data,
        password=None,
        backend=default_backend()
    )
    
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    print("✓ Private key loaded successfully")
    return private_key_der


def create_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Establishes a connection to Snowflake using configuration from SNOWFLAKE_CONFIG.
    
    Supports two authentication methods:
    1. Password authentication: If SNOWFLAKE_PASSWORD is set
    2. Key-pair authentication: If SNOWFLAKE_PASSWORD is None, uses PRIVATE_KEY_PATH
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    
    Raises:
        ValueError: If neither password nor private key is properly configured
        snowflake.connector.errors.Error: If connection fails
    """
    connection_params = SNOWFLAKE_CONFIG.copy()
    
    print("\nConnecting to Snowflake...")
    print(f"  Account: {connection_params['account']}")
    print(f"  User: {connection_params['user']}")
    print(f"  Warehouse: {connection_params['warehouse']}")
    print(f"  Database: {connection_params['database']}")
    print(f"  Schema: {connection_params['schema']}")
    print(f"  Role: {connection_params['role']}")
    
    if SNOWFLAKE_PASSWORD is not None:
        print("  Authentication: Password")
        connection_params['password'] = SNOWFLAKE_PASSWORD
    elif PRIVATE_KEY_PATH:
        print("  Authentication: Key-pair (private key)")
        try:
            private_key_der = load_private_key(PRIVATE_KEY_PATH)
            connection_params['private_key'] = private_key_der
        except FileNotFoundError:
            raise ValueError(f"Private key file not found: {PRIVATE_KEY_PATH}")
        except Exception as e:
            raise ValueError(f"Failed to load private key: {e}")
    else:
        raise ValueError(
            "No authentication method configured. "
            "Please set either SNOWFLAKE_PASSWORD or PRIVATE_KEY_PATH"
        )
    
    conn = snowflake.connector.connect(**connection_params)
    print("✓ Successfully connected to Snowflake\n")
    
    return conn


# ============================================================================
# QUERY EXECUTION & METRICS COLLECTION
# ============================================================================

def execute_query_with_metrics(
    conn: snowflake.connector.SnowflakeConnection,
    query_sql: str,
    query_name: str
) -> Dict[str, Any]:
    """
    Executes a Snowflake query with cache disabled and collects performance metrics.
    
    Args:
        conn: Active Snowflake connection
        query_sql: SQL query to execute
        query_name: Descriptive name for logging
    
    Returns:
        Dict containing:
            - 'query_id': Snowflake query ID
            - 'python_time': Execution time measured by Python (seconds)
            - 'result_df': Pandas DataFrame with query results
            - 'metrics': Dict with Snowflake metrics
    """
    cursor = conn.cursor()
    
    try:
        # Disable result caching
        print(f"[{query_name}] Disabling result cache...")
        cursor.execute(DISABLE_CACHE_SQL)
        
        # Execute and time the query
        print(f"[{query_name}] Executing query...")
        start_time = time.perf_counter()
        
        cursor.execute(query_sql)
        result_df = cursor.fetch_pandas_all()
        
        end_time = time.perf_counter()
        python_execution_time = end_time - start_time
        
        print(f"[{query_name}] ✓ Query completed in {python_execution_time:.3f} seconds")
        print(f"[{query_name}]   Rows returned: {len(result_df):,}")
        
        # Get query ID
        cursor.execute(GET_LAST_QUERY_ID_SQL)
        query_id_result = cursor.fetchone()
        query_id = query_id_result[0] if query_id_result else None
        
        if not query_id:
            raise ValueError("Failed to retrieve query ID")
        
        print(f"[{query_name}]   Query ID: {query_id}")
        
        # Fetch metrics from query history
        print(f"[{query_name}] Fetching metrics from query history...")
        cursor.execute(GET_QUERY_METRICS_SQL, (query_id,))
        metrics_row = cursor.fetchone()
        
        if not metrics_row:
            raise ValueError(f"No metrics found for query ID: {query_id}")
        
        metrics = {
            'query_id': metrics_row[0],
            'execution_time': metrics_row[1],
            'total_elapsed_time': metrics_row[2],
            'bytes_scanned': metrics_row[3],
            'rows_produced': metrics_row[4]
        }
        
        print(f"[{query_name}] ✓ Metrics retrieved")
        print(f"[{query_name}]   Execution time (Snowflake): {metrics['execution_time']} ms")
        print(f"[{query_name}]   Bytes scanned: {metrics['bytes_scanned']:,}")
        print()
        
        return {
            'query_id': query_id,
            'python_time': python_execution_time,
            'result_df': result_df,
            'metrics': metrics
        }
    
    finally:
        cursor.close()


# ============================================================================
# RESULT VALIDATION
# ============================================================================

def validate_results(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    name1: str = "Model 1",
    name2: str = "Model 2"
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validates that two query results are identical (order-independent).
    
    Checks:
    1. Same number of rows
    2. Same columns (names and order)
    3. Same values (after sorting to ignore row order)
    
    Args:
        df1: First DataFrame to compare
        df2: Second DataFrame to compare
        name1: Name of first model for logging
        name2: Name of second model for logging
    
    Returns:
        Tuple of (bool, Dict):
            - bool: True if results are identical, False otherwise
            - Dict: Validation details including any differences found
    """
    print("=" * 80)
    print("VALIDATING RESULTS")
    print("=" * 80)
    
    validation_details = {
        'row_count_match': False,
        'column_match': False,
        'values_match': False,
        'details': []
    }
    
    # Check 1: Row count
    print(f"\n1. Checking row counts...")
    rows1, rows2 = len(df1), len(df2)
    print(f"   {name1}: {rows1:,} rows")
    print(f"   {name2}: {rows2:,} rows")
    
    if rows1 == rows2:
        print("   ✓ Row counts match")
        validation_details['row_count_match'] = True
    else:
        print(f"   ✗ Row counts differ by {abs(rows1 - rows2):,} rows")
        validation_details['details'].append(f"Row count mismatch: {rows1} vs {rows2}")
        return False, validation_details
    
    # Check 2: Columns
    print(f"\n2. Checking columns...")
    cols1, cols2 = list(df1.columns), list(df2.columns)
    print(f"   {name1}: {len(cols1)} columns")
    print(f"   {name2}: {len(cols2)} columns")
    
    if cols1 == cols2:
        print("   ✓ Columns match")
        validation_details['column_match'] = True
    else:
        print("   ✗ Columns differ")
        if set(cols1) != set(cols2):
            missing_in_2 = set(cols1) - set(cols2)
            missing_in_1 = set(cols2) - set(cols1)
            if missing_in_2:
                print(f"   Missing in {name2}: {missing_in_2}")
            if missing_in_1:
                print(f"   Missing in {name1}: {missing_in_1}")
        else:
            print(f"   Column order differs")
            print(f"   {name1}: {cols1}")
            print(f"   {name2}: {cols2}")
        validation_details['details'].append("Column mismatch")
        return False, validation_details
    
    # Check 3: Values (order-independent comparison)
    print(f"\n3. Checking values (order-independent)...")
    
    df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
    
    if df1_sorted.equals(df2_sorted):
        print("   ✓ All values match (order-independent)")
        validation_details['values_match'] = True
    else:
        print("   ✗ Values differ")
        comparison = df1_sorted.compare(df2_sorted)
        if not comparison.empty:
            print(f"   Found {len(comparison)} rows with differences")
            validation_details['details'].append(f"{len(comparison)} rows with value differences")
        else:
            validation_details['details'].append("Data type or floating point precision differences")
        return False, validation_details
    
    print("\n" + "=" * 80)
    print("✓ VALIDATION PASSED - Results are identical")
    print("=" * 80 + "\n")
    
    return True, validation_details


# ============================================================================
# PERFORMANCE COMPARISON
# ============================================================================

def print_performance_comparison(
    model1_results: Dict[str, Any],
    model2_results: Dict[str, Any],
    model1_name: str,
    model2_name: str
) -> None:
    """
    Prints a formatted table comparing performance metrics of both models.
    
    Args:
        model1_results: Results dictionary from first model execution
        model2_results: Results dictionary from second model execution
        model1_name: Name of first model
        model2_name: Name of second model
    """
    print("=" * 120)
    print("PERFORMANCE COMPARISON")
    print("=" * 120)
    
    metrics1 = model1_results['metrics']
    metrics2 = model2_results['metrics']
    
    # Print header
    header = f"{'Model':<30} | {'Python Time (s)':<15} | {'Execution Time (ms)':<20} | {'Bytes Scanned':<15} | {'Rows':<10}"
    print(header)
    print("-" * 120)
    
    # Print model 1 metrics
    row1 = (
        f"{model1_name:<30} | "
        f"{model1_results['python_time']:<15.3f} | "
        f"{metrics1['execution_time']:<20,} | "
        f"{metrics1['bytes_scanned']:<15,} | "
        f"{metrics1['rows_produced']:<10,}"
    )
    print(row1)
    
    # Print model 2 metrics
    row2 = (
        f"{model2_name:<30} | "
        f"{model2_results['python_time']:<15.3f} | "
        f"{metrics2['execution_time']:<20,} | "
        f"{metrics2['bytes_scanned']:<15,} | "
        f"{metrics2['rows_produced']:<10,}"
    )
    print(row2)
    
    print("-" * 120)
    
    # Calculate and print performance improvements
    print(f"\nPERFORMANCE IMPROVEMENT ({model2_name} vs {model1_name}):")
    
    # Python execution time improvement
    time_improvement = (
        (model1_results['python_time'] - model2_results['python_time']) 
        / model1_results['python_time'] * 100
    )
    print(f"  Python Time: {time_improvement:+.2f}% ({'faster' if time_improvement > 0 else 'slower'})")
    
    # Snowflake execution time improvement
    sf_time_improvement = (
        (metrics1['execution_time'] - metrics2['execution_time']) 
        / metrics1['execution_time'] * 100
    )
    print(f"  Snowflake Execution Time: {sf_time_improvement:+.2f}% ({'faster' if sf_time_improvement > 0 else 'slower'})")
    
    # Bytes scanned improvement
    bytes_improvement = (
        (metrics1['bytes_scanned'] - metrics2['bytes_scanned']) 
        / metrics1['bytes_scanned'] * 100
    )
    print(f"  Bytes Scanned: {bytes_improvement:+.2f}% ({'less' if bytes_improvement > 0 else 'more'} data scanned)")
    
    print("=" * 120 + "\n")


# ============================================================================
# JSON OUTPUT
# ============================================================================

def save_results_to_json(
    model1_results: Dict[str, Any],
    model2_results: Dict[str, Any],
    model1_name: str,
    model2_name: str,
    validation_passed: bool,
    validation_details: Dict[str, Any],
    output_path: str = OUTPUT_JSON_PATH
) -> None:
    """
    Saves comparison results to a JSON file.
    
    Args:
        model1_results: Results from first model
        model2_results: Results from second model
        model1_name: Name of first model
        model2_name: Name of second model
        validation_passed: Whether validation passed
        validation_details: Details about validation checks
        output_path: Path where JSON file should be saved
    """
    output = {
        'model_1': {
            'name': model1_name,
            'query_id': model1_results['query_id'],
            'python_time': model1_results['python_time'],
            'metrics': model1_results['metrics']
        },
        'model_2': {
            'name': model2_name,
            'query_id': model2_results['query_id'],
            'python_time': model2_results['python_time'],
            'metrics': model2_results['metrics']
        },
        'validation': {
            'pass': validation_passed,
            'details': validation_details
        }
    }
    
    print(f"Saving results to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"✓ Results saved to {output_path}\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that orchestrates the entire DBT model comparison process.
    
    Process:
    1. Compile both DBT models to SQL
    2. Connect to Snowflake
    3. Execute both models with metrics collection
    4. Validate results match
    5. Compare performance
    6. Save results to JSON
    """
    print("\n" + "=" * 80)
    print("DBT MODEL COMPARISON TOOL")
    print("=" * 80 + "\n")
    
    try:
        # Get model names for display
        model1_name = get_dbt_model_name(MODEL_1_PATH)
        model2_name = get_dbt_model_name(MODEL_2_PATH)
        
        print(f"Comparing DBT models:")
        print(f"  Model 1: {MODEL_1_PATH}")
        print(f"  Model 2: {MODEL_2_PATH}")
        print()
        
        # Step 1: Compile both DBT models
        print("STEP 1: Compiling DBT models")
        print("-" * 80)
        
        model1_sql = compile_dbt_model(MODEL_1_PATH)
        model2_sql = compile_dbt_model(MODEL_2_PATH)
        
        # Step 2: Establish Snowflake connection
        print("\nSTEP 2: Connecting to Snowflake")
        print("-" * 80)
        conn = create_snowflake_connection()
        
        try:
            # Step 3: Execute both models and collect metrics
            print("STEP 3: Executing models and collecting metrics")
            print("-" * 80)
            print()
            
            # Execute model 1
            model1_results = execute_query_with_metrics(
                conn,
                model1_sql,
                model1_name
            )
            
            # Execute model 2
            model2_results = execute_query_with_metrics(
                conn,
                model2_sql,
                model2_name
            )
            
            # Step 4: Validate that results are identical
            print("STEP 4: Validating results")
            print("-" * 80)
            validation_passed, validation_details = validate_results(
                model1_results['result_df'],
                model2_results['result_df'],
                model1_name,
                model2_name
            )
            
            # Step 5: Print performance comparison
            print("STEP 5: Performance comparison")
            print("-" * 80)
            print_performance_comparison(
                model1_results,
                model2_results,
                model1_name,
                model2_name
            )
            
            # Step 6: Save results to JSON file
            print("STEP 6: Saving results")
            print("-" * 80)
            save_results_to_json(
                model1_results,
                model2_results,
                model1_name,
                model2_name,
                validation_passed,
                validation_details
            )
            
            # Final summary
            print("=" * 80)
            print("SUMMARY")
            print("=" * 80)
            print(f"Model 1: {model1_name}")
            print(f"Model 2: {model2_name}")
            print(f"Validation: {'✓ PASS' if validation_passed else '✗ FAIL'}")
            print(f"Output saved to: {OUTPUT_JSON_PATH}")
            print("=" * 80 + "\n")
            
            return 0 if validation_passed else 1
            
        finally:
            print("Closing Snowflake connection...")
            conn.close()
            print("✓ Connection closed\n")
    
    except FileNotFoundError as e:
        print(f"\n✗ ERROR: File not found: {e}")
        print("\nPlease ensure:")
        print("  - DBT project directory is correct")
        print("  - Model paths are valid")
        print("  - Private key file exists (if using key-pair authentication)")
        return 1
    except ValueError as e:
        print(f"\n✗ ERROR: Configuration error: {e}")
        print("\nPlease check:")
        print("  - SNOWFLAKE_CONFIG has all required fields")
        print("  - Either SNOWFLAKE_PASSWORD or PRIVATE_KEY_PATH is properly configured")
        return 1
    except RuntimeError as e:
        print(f"\n✗ ERROR: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    """
    Script entry point. Executes main() and exits with appropriate code.
    
    Usage:
        python dbt_model_compare.py
    
    Configuration:
        Edit the following constants in the script:
        - SNOWFLAKE_CONFIG: Account, user, warehouse, database, schema, role
        - SNOWFLAKE_PASSWORD or PRIVATE_KEY_PATH: Authentication method
        - DBT_PROJECT_DIR: Path to your DBT project
        - MODEL_1_PATH: First model to compare (e.g., "cisco/query2_sales_revenue")
        - MODEL_2_PATH: Second model to compare (e.g., "cisco/query2_sales_revenue_optimized")
    
    Requirements:
        - dbt-core and dbt-snowflake installed
        - DBT profiles configured (~/.dbt/profiles.yml)
        - Snowflake credentials configured
    
    Output:
        - Console output with detailed progress and results
        - dbt_model_comparison_results.json with metrics
    """
    exit(main())
