# import os
# import pandas as pd
# from pathlib import Path
# from typing import Optional, List, Dict, Any
# import glob
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from threading import Lock
# import time
# from datetime import datetime
#
# # Lock for thread-safe printing
# print_lock = Lock()
#
#
# def thread_safe_print(message: str):
#     """Thread-safe printing function"""
#     with print_lock:
#         print(message)
#
#
# def process_single_symbol(
#         symbol: str,
#         raw_data_path: str,
#         output_path: str,
#         start_year: int,
#         end_year: int,
#         symbol_index: int,
#         total_symbols: int
# ) -> tuple[str, dict]:
#     """
#     Process a single stock symbol - designed to be run in parallel
#
#     Returns:
#     --------
#     tuple: (symbol, result_dict)
#     """
#     thread_safe_print(f"[{symbol_index}/{total_symbols}] Processing {symbol}...")
#
#     all_dfs = []
#     years_found = []
#
#     # Collect all CSV files for this symbol across all years
#     for year in range(start_year, end_year + 1):
#         # Construct the path pattern for CSV files
#         pattern = os.path.join(
#             raw_data_path,
#             str(year),
#             'equity',
#             symbol,
#             f'{year}-*.csv'
#         )
#
#         # Find all matching CSV files for this year
#         csv_files = glob.glob(pattern)
#
#         for csv_file in csv_files:
#             try:
#                 # Read the CSV file
#                 df = pd.read_csv(csv_file)
#
#                 # Add a column to track the source file/year if needed
#                 df['source_year'] = year
#                 df['source_file'] = os.path.basename(csv_file)
#
#                 all_dfs.append(df)
#                 years_found.append(year)
#                 thread_safe_print(
#                     f"  [{symbol_index}/{total_symbols}] {symbol}: Loaded {os.path.basename(csv_file)}: {len(df)} rows")
#
#             except Exception as e:
#                 thread_safe_print(f"  [{symbol_index}/{total_symbols}] {symbol}: Error reading {csv_file}: {str(e)}")
#
#     # Aggregate and save if data was found
#     if all_dfs:
#         try:
#             # Concatenate all dataframes
#             combined_df = pd.concat(all_dfs, ignore_index=True)
#
#             # Sort by date if there's a date column
#             date_columns = ['Date', 'date', 'DATE', 'timestamp', 'Timestamp']
#             for col in date_columns:
#                 if col in combined_df.columns:
#                     combined_df[col] = pd.to_datetime(combined_df[col])
#                     combined_df = combined_df.sort_values(col)
#                     break
#
#             # Remove duplicate rows if any (keeping the first occurrence)
#             initial_rows = len(combined_df)
#             combined_df = combined_df.drop_duplicates()
#             duplicates_removed = initial_rows - len(combined_df)
#
#             # Save to CSV
#             output_file = os.path.join(output_path, f"{symbol}_aggregated.csv")
#             combined_df.to_csv(output_file, index=False)
#
#             result = {
#                 'status': 'success',
#                 'file_path': output_file,
#                 'total_rows': len(combined_df),
#                 'years_included': sorted(list(set(years_found))),
#                 'duplicates_removed': duplicates_removed
#             }
#
#             thread_safe_print(
#                 f"  ✓ [{symbol_index}/{total_symbols}] {symbol}: Saved {len(combined_df)} rows from years {sorted(list(set(years_found)))}")
#
#             return symbol, result
#
#         except Exception as e:
#             result = {
#                 'status': 'error',
#                 'error': str(e)
#             }
#             thread_safe_print(f"  ✗ [{symbol_index}/{total_symbols}] {symbol}: Error aggregating - {str(e)}")
#             return symbol, result
#     else:
#         result = {
#             'status': 'no_data',
#             'message': 'No CSV files found for this symbol'
#         }
#         thread_safe_print(f"  - [{symbol_index}/{total_symbols}] {symbol}: No data found")
#         return symbol, result
#
#
# def aggregate_stock_data(
#         raw_data_path: str,
#         output_path: str,
#         stock_symbols: Optional[List[str]] = None,
#         start_year: int = 2018,
#         end_year: int = 2025,
#         max_workers: int = 10
# ) -> dict:
#     """
#     Aggregates all yearly CSV files for each stock symbol into single CSV files.
#     Uses multi-threading for parallel processing of multiple symbols.
#
#     Parameters:
#     -----------
#     raw_data_path : str
#         Path to the raw data folder (e.g., '/Users/darshkachhara/TradingTerminal/data/raw')
#     output_path : str
#         Path where aggregated files will be saved
#     stock_symbols : List[str], optional
#         List of specific stock symbols to process. If None, processes all found symbols
#     start_year : int
#         Starting year to include (default: 2018)
#     end_year : int
#         Ending year to include (default: 2025)
#     max_workers : int
#         Maximum number of parallel threads (default: 10)
#
#     Returns:
#     --------
#     dict : Dictionary with stock symbols as keys and status/file paths as values
#     """
#
#     start_time = time.time()
#
#     # Create output directory if it doesn't exist
#     Path(output_path).mkdir(parents=True, exist_ok=True)
#
#     # If no specific symbols provided, find all available symbols
#     if stock_symbols is None:
#         print("Discovering all stock symbols...")
#         stock_symbols = set()
#         for year in range(start_year, end_year + 1):
#             equity_path = os.path.join(raw_data_path, str(year), 'equity')
#             if os.path.exists(equity_path):
#                 # Get all subdirectories (stock symbols) in this year's equity folder
#                 for symbol in os.listdir(equity_path):
#                     symbol_path = os.path.join(equity_path, symbol)
#                     if os.path.isdir(symbol_path):
#                         stock_symbols.add(symbol)
#         stock_symbols = sorted(list(stock_symbols))
#
#     total_symbols = len(stock_symbols)
#     print(f"\nFound {total_symbols} unique stock symbols to process")
#     print(f"Using {max_workers} parallel workers")
#     print("=" * 60)
#
#     # Dictionary to store results
#     results = {}
#
#     # Process symbols in parallel using ThreadPoolExecutor
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # Submit all tasks
#         futures = {
#             executor.submit(
#                 process_single_symbol,
#                 symbol,
#                 raw_data_path,
#                 output_path,
#                 start_year,
#                 end_year,
#                 idx + 1,
#                 total_symbols
#             ): symbol
#             for idx, symbol in enumerate(stock_symbols)
#         }
#
#         # Collect results as they complete
#         completed = 0
#         for future in as_completed(futures):
#             completed += 1
#             try:
#                 symbol, result = future.result()
#                 results[symbol] = result
#
#                 # Print progress
#                 if completed % 10 == 0 or completed == total_symbols:
#                     elapsed = time.time() - start_time
#                     rate = completed / elapsed
#                     eta = (total_symbols - completed) / rate if rate > 0 else 0
#                     thread_safe_print(f"\n>>> Progress: {completed}/{total_symbols} symbols completed "
#                                       f"({completed * 100 / total_symbols:.1f}%) - "
#                                       f"ETA: {eta:.1f}s\n")
#
#             except Exception as e:
#                 symbol = futures[future]
#                 results[symbol] = {
#                     'status': 'error',
#                     'error': f'Thread execution error: {str(e)}'
#                 }
#                 thread_safe_print(f"Error processing {symbol}: {str(e)}")
#
#     # Calculate processing time
#     total_time = time.time() - start_time
#
#     # Print summary
#     print("\n" + "=" * 60)
#     print("AGGREGATION SUMMARY")
#     print("=" * 60)
#
#     successful = sum(1 for r in results.values() if r['status'] == 'success')
#     failed = sum(1 for r in results.values() if r['status'] == 'error')
#     no_data = sum(1 for r in results.values() if r['status'] == 'no_data')
#
#     print(f"Total symbols processed: {len(results)}")
#     print(f"Successful: {successful}")
#     print(f"Failed: {failed}")
#     print(f"No data found: {no_data}")
#     print(f"\nTotal processing time: {total_time:.2f} seconds")
#     print(f"Average time per symbol: {total_time / len(results):.2f} seconds")
#
#     # Save a summary report
#     summary_file = os.path.join(output_path, f"aggregation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
#     with open(summary_file, 'w') as f:
#         f.write(f"Stock Data Aggregation Summary\n")
#         f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
#         f.write(f"{'=' * 60}\n\n")
#         f.write(f"Total symbols processed: {len(results)}\n")
#         f.write(f"Successful: {successful}\n")
#         f.write(f"Failed: {failed}\n")
#         f.write(f"No data found: {no_data}\n")
#         f.write(f"Processing time: {total_time:.2f} seconds\n")
#         f.write(f"Years covered: {start_year} to {end_year}\n\n")
#
#         f.write("Successful aggregations:\n")
#         f.write("-" * 40 + "\n")
#         for symbol, result in sorted(results.items()):
#             if result['status'] == 'success':
#                 f.write(f"{symbol}: {result['total_rows']} rows, "
#                         f"years {result['years_included']}\n")
#
#         if failed > 0:
#             f.write("\nFailed aggregations:\n")
#             f.write("-" * 40 + "\n")
#             for symbol, result in sorted(results.items()):
#                 if result['status'] == 'error':
#                     f.write(f"{symbol}: {result.get('error', 'Unknown error')}\n")
#
#         if no_data > 0:
#             f.write("\nNo data found for:\n")
#             f.write("-" * 40 + "\n")
#             for symbol, result in sorted(results.items()):
#                 if result['status'] == 'no_data':
#                     f.write(f"{symbol}\n")
#
#     print(f"\nSummary report saved to: {summary_file}")
#
#     return results
#
#
# # Example usage
# if __name__ == "__main__":
#     # Define your paths
#     raw_data_path = "/Users/darshkachhara/TradingTerminal/data/raw"
#     output_path = "/Users/darshkachhara/TradingTerminal/data/aggregated"
#
#     # Option 1: Process all available symbols with default 10 workers
#     results = aggregate_stock_data(
#         raw_data_path=raw_data_path,
#         output_path=output_path,
#         max_workers=10  # Adjust based on your system capabilities
#     )
#
#     # Option 2: Process with more workers for faster processing
#     # results = aggregate_stock_data(
#     #     raw_data_path=raw_data_path,
#     #     output_path=output_path,
#     #     max_workers=20  # Increase for faster processing if you have many cores
#     # )
#
#     # Option 3: Process specific symbols only with custom settings
#     # specific_symbols = ['AARTIIND', 'RELIANCE', 'TCS', 'INFY', 'WIPRO']
#     # results = aggregate_stock_data(
#     #     raw_data_path=raw_data_path,
#     #     output_path=output_path,
#     #     stock_symbols=specific_symbols,
#     #     start_year=2020,
#     #     end_year=2024,
#     #     max_workers=5
#     # )

import os
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from datetime import datetime

# Lock for thread-safe printing
print_lock = Lock()


def thread_safe_print(message: str):
    """Thread-safe printing function"""
    with print_lock:
        print(message)


def process_single_index(
        index_name: str,
        raw_data_path: str,
        output_path: str,
        start_year: int,
        end_year: int,
        index_number: int,
        total_indices: int
) -> tuple[str, dict]:
    """
    Process a single index - designed to be run in parallel

    Returns:
    --------
    tuple: (index_name, result_dict)
    """
    thread_safe_print(f"[{index_number}/{total_indices}] Processing {index_name}...")

    all_dfs = []
    years_found = []

    # Collect all CSV files for this index across all years
    for year in range(start_year, end_year + 1):
        # Construct the path pattern for CSV files
        pattern = os.path.join(
            raw_data_path,
            str(year),
            'index',  # Changed from 'equity' to 'index'
            index_name,
            f'{year}-*.csv'
        )

        # Find all matching CSV files for this year
        csv_files = glob.glob(pattern)

        for csv_file in csv_files:
            try:
                # Read the CSV file
                df = pd.read_csv(csv_file)

                # Add a column to track the source file/year if needed
                df['source_year'] = year
                df['source_file'] = os.path.basename(csv_file)

                all_dfs.append(df)
                years_found.append(year)
                thread_safe_print(
                    f"  [{index_number}/{total_indices}] {index_name}: Loaded {os.path.basename(csv_file)}: {len(df)} rows")

            except Exception as e:
                thread_safe_print(
                    f"  [{index_number}/{total_indices}] {index_name}: Error reading {csv_file}: {str(e)}")

    # Aggregate and save if data was found
    if all_dfs:
        try:
            # Concatenate all dataframes
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Sort by date if there's a date column
            date_columns = ['Date', 'date', 'DATE', 'timestamp', 'Timestamp']
            for col in date_columns:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_datetime(combined_df[col])
                    combined_df = combined_df.sort_values(col)
                    break

            # Remove duplicate rows if any (keeping the first occurrence)
            initial_rows = len(combined_df)
            combined_df = combined_df.drop_duplicates()
            duplicates_removed = initial_rows - len(combined_df)

            # Save to CSV - clean up the filename for indices with spaces
            safe_filename = index_name.replace(' ', '_').replace('/', '_')
            output_file = os.path.join(output_path, f"{safe_filename}_aggregated.csv")
            combined_df.to_csv(output_file, index=False)

            result = {
                'status': 'success',
                'file_path': output_file,
                'total_rows': len(combined_df),
                'years_included': sorted(list(set(years_found))),
                'duplicates_removed': duplicates_removed
            }

            thread_safe_print(
                f"  ✓ [{index_number}/{total_indices}] {index_name}: Saved {len(combined_df)} rows from years {sorted(list(set(years_found)))}")

            return index_name, result

        except Exception as e:
            result = {
                'status': 'error',
                'error': str(e)
            }
            thread_safe_print(f"  ✗ [{index_number}/{total_indices}] {index_name}: Error aggregating - {str(e)}")
            return index_name, result
    else:
        result = {
            'status': 'no_data',
            'message': 'No CSV files found for this index'
        }
        thread_safe_print(f"  - [{index_number}/{total_indices}] {index_name}: No data found")
        return index_name, result


def aggregate_index_data(
        raw_data_path: str,
        output_path: str,
        index_names: Optional[List[str]] = None,
        start_year: int = 2018,
        end_year: int = 2025,
        max_workers: int = 10
) -> dict:
    """
    Aggregates all yearly CSV files for each index into single CSV files.
    Uses multi-threading for parallel processing of multiple indices.

    Parameters:
    -----------
    raw_data_path : str
        Path to the raw data folder (e.g., '/Users/darshkachhara/TradingTerminal/data/raw')
    output_path : str
        Path where aggregated files will be saved
    index_names : List[str], optional
        List of specific index names to process. If None, processes all found indices
    start_year : int
        Starting year to include (default: 2018)
    end_year : int
        Ending year to include (default: 2025)
    max_workers : int
        Maximum number of parallel threads (default: 10)

    Returns:
    --------
    dict : Dictionary with index names as keys and status/file paths as values
    """

    start_time = time.time()

    # Create output directory if it doesn't exist
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # Default index names if not provided
    if index_names is None:
        # Try to discover all indices, but also include these common ones
        default_indices = [
            'BANKNIFTY',
            'NIFTY 50',
            'NIFTY FIN SERVICE',
            'niftymidcap',
            'SENSEX'
        ]

        print("Discovering all index names...")
        discovered_indices = set()

        for year in range(start_year, end_year + 1):
            index_path = os.path.join(raw_data_path, str(year), 'index')
            if os.path.exists(index_path):
                # Get all subdirectories (index names) in this year's index folder
                for index_name in os.listdir(index_path):
                    index_subpath = os.path.join(index_path, index_name)
                    if os.path.isdir(index_subpath):
                        discovered_indices.add(index_name)

        # Combine discovered and default indices
        all_indices = discovered_indices.union(set(default_indices))
        index_names = sorted(list(all_indices))

        print(f"Found indices: {index_names}")

    total_indices = len(index_names)
    print(f"\nProcessing {total_indices} indices")
    print(f"Using {max_workers} parallel workers")
    print("=" * 60)

    # Dictionary to store results
    results = {}

    # Process indices in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_single_index,
                index_name,
                raw_data_path,
                output_path,
                start_year,
                end_year,
                idx + 1,
                total_indices
            ): index_name
            for idx, index_name in enumerate(index_names)
        }

        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            completed += 1
            try:
                index_name, result = future.result()
                results[index_name] = result

                # Print progress
                if completed % 5 == 0 or completed == total_indices:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    eta = (total_indices - completed) / rate if rate > 0 else 0
                    thread_safe_print(f"\n>>> Progress: {completed}/{total_indices} indices completed "
                                      f"({completed * 100 / total_indices:.1f}%) - "
                                      f"ETA: {eta:.1f}s\n")

            except Exception as e:
                index_name = futures[future]
                results[index_name] = {
                    'status': 'error',
                    'error': f'Thread execution error: {str(e)}'
                }
                thread_safe_print(f"Error processing {index_name}: {str(e)}")

    # Calculate processing time
    total_time = time.time() - start_time

    # Print summary
    print("\n" + "=" * 60)
    print("INDEX AGGREGATION SUMMARY")
    print("=" * 60)

    successful = sum(1 for r in results.values() if r['status'] == 'success')
    failed = sum(1 for r in results.values() if r['status'] == 'error')
    no_data = sum(1 for r in results.values() if r['status'] == 'no_data')

    print(f"Total indices processed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"No data found: {no_data}")
    print(f"\nTotal processing time: {total_time:.2f} seconds")
    print(f"Average time per index: {total_time / len(results) if results else 0:.2f} seconds")

    # Save a summary report
    summary_file = os.path.join(output_path,
                                f"index_aggregation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(summary_file, 'w') as f:
        f.write(f"Index Data Aggregation Summary\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Total indices processed: {len(results)}\n")
        f.write(f"Successful: {successful}\n")
        f.write(f"Failed: {failed}\n")
        f.write(f"No data found: {no_data}\n")
        f.write(f"Processing time: {total_time:.2f} seconds\n")
        f.write(f"Years covered: {start_year} to {end_year}\n\n")

        f.write("Successful aggregations:\n")
        f.write("-" * 40 + "\n")
        for index_name, result in sorted(results.items()):
            if result['status'] == 'success':
                f.write(f"{index_name}: {result['total_rows']} rows, "
                        f"years {result['years_included']}\n")

        if failed > 0:
            f.write("\nFailed aggregations:\n")
            f.write("-" * 40 + "\n")
            for index_name, result in sorted(results.items()):
                if result['status'] == 'error':
                    f.write(f"{index_name}: {result.get('error', 'Unknown error')}\n")

        if no_data > 0:
            f.write("\nNo data found for:\n")
            f.write("-" * 40 + "\n")
            for index_name, result in sorted(results.items()):
                if result['status'] == 'no_data':
                    f.write(f"{index_name}\n")

    print(f"\nSummary report saved to: {summary_file}")

    # Print successful output files
    if successful > 0:
        print("\nGenerated files:")
        for index_name, result in sorted(results.items()):
            if result['status'] == 'success':
                filename = os.path.basename(result['file_path'])
                print(f"  - {filename}")

    return results


# Example usage
if __name__ == "__main__":
    # Define your paths
    raw_data_path = "/Users/darshkachhara/TradingTerminal/data/raw"
    output_path = "/Users/darshkachhara/TradingTerminal/data/aggregated_indices"

    # Option 1: Process default indices (BANKNIFTY, NIFTY 50, etc.)
    results = aggregate_index_data(
        raw_data_path=raw_data_path,
        output_path=output_path,
        max_workers=5  # Fewer workers since we have fewer indices than stocks
    )

    # Option 2: Process specific indices only
    # specific_indices = ['BANKNIFTY', 'NIFTY 50', 'SENSEX']
    # results = aggregate_index_data(
    #     raw_data_path=raw_data_path,
    #     output_path=output_path,
    #     index_names=specific_indices,
    #     max_workers=3
    # )

    # Option 3: Process all discovered indices with custom year range
    # results = aggregate_index_data(
    #     raw_data_path=raw_data_path,
    #     output_path=output_path,
    #     index_names=None,  # Will auto-discover all indices
    #     start_year=2020,
    #     end_year=2024,
    #     max_workers=5
    # )