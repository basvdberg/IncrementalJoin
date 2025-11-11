"""
Helper script to start Spark Connect server for Windows.
Use this if you installed PySpark via pip and don't have a full Spark installation.

Usage:
    python tests/start_spark_connect_server.py

The server will start on port 15002 by default.
Stop it with Ctrl+C.
"""

import os
import sys

try:
    from pyspark.sql.connect.service import SparkConnectService

    def main():
        port = int(os.environ.get("SPARK_CONNECT_PORT", "15002"))
        print(f"Starting Spark Connect server on port {port}...")
        print("Press Ctrl+C to stop the server")

        service = SparkConnectService(port=port)
        service.start()

except ImportError as e:
    print("Error: Could not import SparkConnectService.")
    print("This script requires PySpark 3.4+ with Spark Connect support.")
    print(f"Import error: {e}")
    print("\nAlternative: Use Option 2 (Spark Standalone) instead.")
    print("Or install a full Spark distribution with SPARK_HOME set.")
    sys.exit(1)
except Exception as e:
    print(f"Error starting Spark Connect server: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure PySpark 3.4+ is installed: pip install pyspark>=3.4")
    print("2. Check if port 15002 is already in use")
    print("3. Try using Option 2 (Spark Standalone) instead")
    sys.exit(1)

if __name__ == "__main__":
    main()
