#!/usr/bin/env python3
"""
Snowflake Query Performance and Correctness Comparison Script

This script compares two Snowflake SQL queries (optimized vs non-optimized) by:
1. Executing both queries with cache disabled
2. Collecting performance metrics from Snowflake's query history
3. Validating that both queries return identical results
4. Comparing execution times and resource usage
5. Outputting results to console and JSON file

Dependencies:
- snowflake-connector-python
- pandas
- cryptography (for key-pair authentication)
"""

import os
import time
import json
import snowflake.connector
import pandas as pd
from typing import Dict, Any, Tuple
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# IMP: Kindly update the Snowflake config with your account details:
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
# Set this to your password string, or leave as None to use key-pair authentication
SNOWFLAKE_PASSWORD = None  # Example: 'your-password-here'
# Option 2: Key-pair authentication (more secure)
# Path to your private key PEM file
# If SNOWFLAKE_PASSWORD is None, this key file will be used
PRIVATE_KEY_PATH = '/Users/anas/snowflake_key.pem'

# File paths for the SQL queries to compare
OPTIMIZED_QUERY_PATH = "/Users/anas/demos/cisco_demo/jaffle-shop/target/compiled/jaffle_shop_test/models/cisco/query2_sales_revenue_optimized.sql"
NON_OPTIMIZED_QUERY_PATH = "/Users/anas/demos/cisco_demo/jaffle-shop/target/compiled/jaffle_shop_test/models/cisco/query2_sales_revenue.sql"

# Output file for results
OUTPUT_JSON_PATH = "query_comparison_results.json"

# SQL command to disable Snowflake's result caching
DISABLE_CACHE_SQL = "ALTER SESSION SET USE_CACHED_RESULT = FALSE;"

# SQL to retrieve the last executed query ID
GET_LAST_QUERY_ID_SQL = "SELECT LAST_QUERY_ID();"

# SQL to fetch metrics from query history for a specific query ID
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
    
    # Parse the private key (assuming no passphrase)
    # If your key has a passphrase, add: password=b'your-passphrase'
    private_key = serialization.load_pem_private_key(
        private_key_data,
        password=None,  # Change this if your key has a passphrase
        backend=default_backend()
    )
    
    # Convert to DER format (required by snowflake-connector-python)
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    print("✓ Private key loaded successfully")
    return private_key_der


def create_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Establishes a connection to Snowflake using hardcoded configuration.
    
    Supports two authentication methods:
    1. Password authentication: If SNOWFLAKE_PASSWORD is set
    2. Key-pair authentication: If SNOWFLAKE_PASSWORD is None, uses PRIVATE_KEY_PATH
    
    Configuration is read from:
        - SNOWFLAKE_CONFIG: Dictionary with account, user, warehouse, database, schema, role
        - SNOWFLAKE_PASSWORD: Password string (if using password auth)
        - PRIVATE_KEY_PATH: Path to private key PEM file (if using key-pair auth)
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
    
    Raises:
        ValueError: If neither password nor private key is properly configured
        snowflake.connector.errors.Error: If connection fails
    """
    # Start with base connection parameters
    connection_params = SNOWFLAKE_CONFIG.copy()
    
    print("Connecting to Snowflake...")
    print(f"  Account: {connection_params['account']}")
    print(f"  User: {connection_params['user']}")
    print(f"  Warehouse: {connection_params['warehouse']}")
    print(f"  Database: {connection_params['database']}")
    print(f"  Schema: {connection_params['schema']}")
    print(f"  Role: {connection_params['role']}")
    
    # Determine authentication method and add appropriate credentials
    if SNOWFLAKE_PASSWORD is not None:
        # Option 1: Password authentication
        print("  Authentication: Password")
        connection_params['password'] = SNOWFLAKE_PASSWORD
    elif PRIVATE_KEY_PATH:
        # Option 2: Key-pair authentication
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
    
    # Establish the connection
    conn = snowflake.connector.connect(**connection_params)
    print("✓ Successfully connected to Snowflake\n")
    
    return conn


# ============================================================================
# QUERY FILE LOADING
# ============================================================================

def load_sql_file(file_path: str) -> str:
    """
    Reads SQL query content from a file.
    
    Args:
        file_path: Path to the SQL file
    
    Returns:
        str: SQL query content
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If the file cannot be read
    """
    print(f"Loading SQL from: {file_path}")
    with open(file_path, 'r') as f:
        sql_content = f.read().strip()
    print(f"  ✓ Loaded {len(sql_content)} characters\n")
    return sql_content


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
    
    This function:
    1. Disables Snowflake's result cache
    2. Times the query execution using Python's perf_counter
    3. Executes the query and fetches all results into a DataFrame
    4. Retrieves the query ID and fetches detailed metrics from query history
    
    Args:
        conn: Active Snowflake connection
        query_sql: SQL query to execute
        query_name: Descriptive name for logging (e.g., "Optimized" or "Non-Optimized")
    
    Returns:
        Dict containing:
            - 'query_id': Snowflake query ID
            - 'python_time': Execution time measured by Python (seconds)
            - 'result_df': Pandas DataFrame with query results
            - 'metrics': Dict with Snowflake metrics (execution_time, bytes_scanned, etc.)
    """
    cursor = conn.cursor()
    
    try:
        # Step 1: Disable result caching to ensure accurate performance measurements
        print(f"[{query_name}] Disabling result cache...")
        cursor.execute(DISABLE_CACHE_SQL)
        
        # Step 2: Start timing the query execution
        print(f"[{query_name}] Executing query...")
        start_time = time.perf_counter()
        
        # Step 3: Execute the actual query
        cursor.execute(query_sql)
        
        # Step 4: Fetch all results into a Pandas DataFrame
        result_df = cursor.fetch_pandas_all()
        
        # Step 5: Stop timing
        end_time = time.perf_counter()
        python_execution_time = end_time - start_time
        
        print(f"[{query_name}] ✓ Query completed in {python_execution_time:.3f} seconds")
        print(f"[{query_name}]   Rows returned: {len(result_df)}")
        
        # Step 6: Get the query ID of the query we just executed
        cursor.execute(GET_LAST_QUERY_ID_SQL)
        query_id_result = cursor.fetchone()
        query_id = query_id_result[0] if query_id_result else None
        
        if not query_id:
            raise ValueError("Failed to retrieve query ID")
        
        print(f"[{query_name}]   Query ID: {query_id}")
        
        # Step 7: Fetch detailed metrics from Snowflake's query history
        print(f"[{query_name}] Fetching metrics from query history...")
        cursor.execute(GET_QUERY_METRICS_SQL, (query_id,))
        metrics_row = cursor.fetchone()
        
        if not metrics_row:
            raise ValueError(f"No metrics found for query ID: {query_id}")
        
        # Step 8: Parse metrics into a dictionary
        # Columns: query_id, execution_time, total_elapsed_time, bytes_scanned, rows_produced
        metrics = {
            'query_id': metrics_row[0],
            'execution_time': metrics_row[1],  # milliseconds
            'total_elapsed_time': metrics_row[2],  # milliseconds
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
        # Always close the cursor to free resources
        cursor.close()


# ============================================================================
# RESULT VALIDATION
# ============================================================================

def validate_results(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    name1: str = "Optimized",
    name2: str = "Non-Optimized"
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
        name1: Name of first query for logging
        name2: Name of second query for logging
    
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
    
    # Sort both DataFrames by all columns to enable order-independent comparison
    # Reset index after sorting to ensure proper comparison
    df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
    
    # Compare the sorted DataFrames
    if df1_sorted.equals(df2_sorted):
        print("   ✓ All values match (order-independent)")
        validation_details['values_match'] = True
    else:
        print("   ✗ Values differ")
        
        # Find differences for reporting
        comparison = df1_sorted.compare(df2_sorted)
        if not comparison.empty:
            print(f"   Found {len(comparison)} rows with differences")
            validation_details['details'].append(f"{len(comparison)} rows with value differences")
        else:
            # If compare() returns empty but equals() is False, might be data type differences
            validation_details['details'].append("Data type or floating point precision differences")
        
        return False, validation_details
    
    # All checks passed
    print("\n" + "=" * 80)
    print("✓ VALIDATION PASSED - Results are identical")
    print("=" * 80 + "\n")
    
    return True, validation_details


# ============================================================================
# PERFORMANCE COMPARISON
# ============================================================================

def print_performance_comparison(
    optimized_results: Dict[str, Any],
    non_optimized_results: Dict[str, Any]
) -> None:
    """
    Prints a formatted table comparing performance metrics of both queries.
    
    Args:
        optimized_results: Results dictionary from optimized query execution
        non_optimized_results: Results dictionary from non-optimized query execution
    """
    print("=" * 120)
    print("PERFORMANCE COMPARISON")
    print("=" * 120)
    
    # Extract metrics for easier access
    opt_metrics = optimized_results['metrics']
    non_opt_metrics = non_optimized_results['metrics']
    
    # Print header
    header = f"{'Query':<20} | {'Python Time (s)':<15} | {'Execution Time (ms)':<20} | {'Bytes Scanned':<15} | {'Rows':<10} | {'Columns':<8}"
    print(header)
    print("-" * 120)
    
    # Print optimized query metrics
    opt_row = (
        f"{'Optimized':<20} | "
        f"{optimized_results['python_time']:<15.3f} | "
        f"{opt_metrics['execution_time']:<20,} | "
        f"{opt_metrics['bytes_scanned']:<15,} | "
        f"{opt_metrics['rows_produced']:<10,}"
    )
    print(opt_row)
    
    # Print non-optimized query metrics
    non_opt_row = (
        f"{'Non-Optimized':<20} | "
        f"{non_optimized_results['python_time']:<15.3f} | "
        f"{non_opt_metrics['execution_time']:<20,} | "
        f"{non_opt_metrics['bytes_scanned']:<15,} | "
        f"{non_opt_metrics['rows_produced']:<10,}"
    )
    print(non_opt_row)
    
    print("-" * 120)
    
    # Calculate and print performance improvements
    print("\nPERFORMANCE IMPROVEMENT:")
    
    # Python execution time improvement
    time_improvement = (
        (non_optimized_results['python_time'] - optimized_results['python_time']) 
        / non_optimized_results['python_time'] * 100
    )
    print(f"  Python Time: {time_improvement:+.2f}% ({'faster' if time_improvement > 0 else 'slower'})")
    
    # Snowflake execution time improvement
    sf_time_improvement = (
        (non_opt_metrics['execution_time'] - opt_metrics['execution_time']) 
        / non_opt_metrics['execution_time'] * 100
    )
    print(f"  Snowflake Execution Time: {sf_time_improvement:+.2f}% ({'faster' if sf_time_improvement > 0 else 'slower'})")
    
    # Bytes scanned improvement
    bytes_improvement = (
        (non_opt_metrics['bytes_scanned'] - opt_metrics['bytes_scanned']) 
        / non_opt_metrics['bytes_scanned'] * 100
    )
    print(f"  Bytes Scanned: {bytes_improvement:+.2f}% ({'less' if bytes_improvement > 0 else 'more'} data scanned)")
    
    print("=" * 120 + "\n")


# ============================================================================
# JSON OUTPUT
# ============================================================================

def save_results_to_json(
    optimized_results: Dict[str, Any],
    non_optimized_results: Dict[str, Any],
    validation_passed: bool,
    validation_details: Dict[str, Any],
    output_path: str = OUTPUT_JSON_PATH
) -> None:
    """
    Saves comparison results to a JSON file.
    
    Args:
        optimized_results: Results from optimized query
        non_optimized_results: Results from non-optimized query
        validation_passed: Whether validation passed
        validation_details: Details about validation checks
        output_path: Path where JSON file should be saved
    """
    # Prepare the output structure
    output = {
        'optimized': {
            'query_id': optimized_results['query_id'],
            'python_time': optimized_results['python_time'],
            'metrics': optimized_results['metrics']
        },
        'non_optimized': {
            'query_id': non_optimized_results['query_id'],
            'python_time': non_optimized_results['python_time'],
            'metrics': non_optimized_results['metrics']
        },
        'validation': {
            'pass': validation_passed,
            'details': validation_details
        }
    }
    
    # Write to JSON file with pretty formatting
    print(f"Saving results to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"✓ Results saved to {output_path}\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that orchestrates the entire comparison process.
    
    Process:
    1. Load SQL queries from files
    2. Connect to Snowflake
    3. Execute both queries with metrics collection
    4. Validate results match
    5. Compare performance
    6. Save results to JSON
    """
    print("\n" + "=" * 80)
    print("SNOWFLAKE QUERY COMPARISON TOOL")
    print("=" * 80 + "\n")
    
    try:
        # Step 1: Load SQL queries from files
        print("STEP 1: Loading SQL queries")
        print("-" * 80)
        optimized_sql = load_sql_file(OPTIMIZED_QUERY_PATH)
        non_optimized_sql = load_sql_file(NON_OPTIMIZED_QUERY_PATH)
        
        # Step 2: Establish Snowflake connection
        print("STEP 2: Connecting to Snowflake")
        print("-" * 80)
        conn = create_snowflake_connection()
        
        try:
            # Step 3: Execute both queries and collect metrics
            print("STEP 3: Executing queries and collecting metrics")
            print("-" * 80)

            # Execute non-optimized query FIRST
            non_optimized_results = execute_query_with_metrics(
                conn,
                non_optimized_sql,
                "Non-Optimized"
            )

            # Execute optimized query SECOND
            optimized_results = execute_query_with_metrics(
                conn,
                optimized_sql,
                "Optimized"
            )
            
            # Step 4: Validate that results are identical
            print("STEP 4: Validating results")
            print("-" * 80)
            validation_passed, validation_details = validate_results(
                optimized_results['result_df'],
                non_optimized_results['result_df'],
                "Optimized",
                "Non-Optimized"
            )
            
            # Step 5: Print performance comparison
            print("STEP 5: Performance comparison")
            print("-" * 80)
            print_performance_comparison(optimized_results, non_optimized_results)
            
            # Step 6: Save results to JSON file
            print("STEP 6: Saving results")
            print("-" * 80)
            save_results_to_json(
                optimized_results,
                non_optimized_results,
                validation_passed,
                validation_details
            )
            
            # Final summary
            print("=" * 80)
            print("SUMMARY")
            print("=" * 80)
            print(f"Validation: {'✓ PASS' if validation_passed else '✗ FAIL'}")
            print(f"Output saved to: {OUTPUT_JSON_PATH}")
            print("=" * 80 + "\n")
            
            # Exit with appropriate code
            return 0 if validation_passed else 1
            
        finally:
            # Always close the connection
            print("Closing Snowflake connection...")
            conn.close()
            print("✓ Connection closed\n")
    
    except FileNotFoundError as e:
        print(f"\n✗ ERROR: File not found: {e}")
        print("\nPlease ensure:")
        print("  - SQL query files exist at the specified paths")
        print("  - Private key file exists (if using key-pair authentication)")
        return 1
    except ValueError as e:
        print(f"\n✗ ERROR: Configuration error: {e}")
        print("\nPlease check:")
        print("  - SNOWFLAKE_CONFIG has all required fields")
        print("  - Either SNOWFLAKE_PASSWORD or PRIVATE_KEY_PATH is properly configured")
        print("  - Private key file is valid (if using key-pair authentication)")
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
        python compare_snowflake_queries.py
    
    Configuration:
        Edit the following constants in the script:
        - SNOWFLAKE_CONFIG: Account, user, warehouse, database, schema, role
        - SNOWFLAKE_PASSWORD: Set to your password (Option 1)
        - PRIVATE_KEY_PATH: Path to private key PEM file (Option 2)
    
    Input Files Required:
        - query_4_dev_optimized.sql
        - tpch_snowflake_queries/query_4_dev.sql
    
    Output:
        - Console output with detailed progress and results
        - query_comparison_results.json with metrics
    """
    exit(main())