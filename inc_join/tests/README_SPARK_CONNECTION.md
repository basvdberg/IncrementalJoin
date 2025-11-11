# Connecting Tests to External Spark Server

This guide explains how to connect your unit tests to an external Spark server.

## Quick Start (Windows)

**Easiest Option: Local Mode (No setup needed!)**

If you installed PySpark via pip (most common), just run your tests directly:

```powershell
pytest tests/test_inc_join.py
```

That's it! The tests will create their own local Spark sessions automatically. No external server needed.

---

**If you want to use an external Spark server** (requires full Spark installation):

1. **Option 1 - Spark Connect Server** (if you have SPARK_HOME set):
   ```powershell
   # Terminal 1
   cd $env:SPARK_HOME
   .\bin\spark-class.cmd org.apache.spark.sql.connect.service.SparkConnectServer
   
   # Terminal 2
   $env:SPARK_CONNECT_URL = "sc://localhost:15002"
   pytest tests/test_inc_join.py
   ```

2. **Option 2 - Spark Standalone Master** (if you have SPARK_HOME set):
   ```powershell
   # Terminal 1
   cd $env:SPARK_HOME
   .\sbin\start-master.cmd
   
   # Terminal 2
   $env:SPARK_MASTER_URL = "spark://localhost:7077"
   pytest tests/test_inc_join.py
   ```

## Detailed Instructions

### Option 1: Spark Connect (Recommended - Spark 3.4+)

#### Windows

1. **Start Spark Connect Server** in a separate PowerShell or Command Prompt window:
   
   > **⚠️ Important**: Spark Connect server is **NOT available** in pip-installed PySpark.
   > You need a **full Spark installation** with SPARK_HOME set to use Spark Connect.
   > 
   > **For pip-installed PySpark, use Option 2 (Spark Standalone) or Option 3 (Local mode) instead.**
   
   **Method 1 - Using spark-class (Requires full Spark installation with SPARK_HOME)**:
   ```powershell
   # PowerShell - Requires SPARK_HOME to be set
   cd $env:SPARK_HOME
   .\bin\spark-class.cmd org.apache.spark.sql.connect.service.SparkConnectServer
   ```
   
   **Method 2 - Using start-connect-server.cmd (Requires full Spark installation with SPARK_HOME)**:
   ```powershell
   # PowerShell - Requires SPARK_HOME to be set
   cd $env:SPARK_HOME
   .\sbin\start-connect-server.cmd
   ```
   
   > **Note**: 
   > - Default port: `15002`
   > - The server will keep running until you stop it (Ctrl+C)
   > - **If you get "ClassNotFoundException"**, you don't have a full Spark installation
   > - **Solution**: Use Option 2 (Spark Standalone) or Option 3 (Local mode) instead
   > - Spark Connect requires downloading Spark from https://spark.apache.org/downloads.html

2. **Run tests with connection URL** in your test terminal:
   ```powershell
   # PowerShell - Set environment variable for current session
   $env:SPARK_CONNECT_URL = "sc://localhost:15002"
   pytest tests/test_inc_join.py
   
   # Or set it for a single command:
   $env:SPARK_CONNECT_URL = "sc://localhost:15002"; pytest tests/test_inc_join.py
   ```
   
   ```cmd
   # Command Prompt
   set SPARK_CONNECT_URL=sc://localhost:15002
   pytest tests/test_inc_join.py
   ```

#### Linux/Mac

1. **Start Spark Connect Server** in a separate terminal:
   ```bash
   $SPARK_HOME/sbin/start-connect-server.sh
   ```
   Default port: `15002`

2. **Run tests with connection URL**:
   ```bash
   export SPARK_CONNECT_URL="sc://localhost:15002"
   pytest tests/test_inc_join.py
   ```

### Option 2: Spark Standalone Cluster (Recommended for pip-installed PySpark)

#### Windows

**Note**: Spark Standalone also requires a full Spark installation. If you only have pip-installed PySpark, **use Option 3 (Local mode)** instead, which works out of the box.

1. **Start Spark Master** in a separate PowerShell or Command Prompt window:
   ```powershell
   # PowerShell - Requires SPARK_HOME to be set
   cd $env:SPARK_HOME
   .\sbin\start-master.cmd
   ```
   Note the master URL from the output (usually `spark://hostname:7077`)

2. **Run tests with master URL**:
   ```powershell
   # PowerShell
   $env:SPARK_MASTER_URL = "spark://localhost:7077"
   pytest tests/test_inc_join.py
   ```
   
   ```cmd
   # Command Prompt
   set SPARK_MASTER_URL=spark://localhost:7077
   pytest tests/test_inc_join.py
   ```

#### Linux/Mac

1. **Start Spark Master**:
   ```bash
   $SPARK_HOME/sbin/start-master.sh
   ```
   Note the master URL (usually `spark://hostname:7077`)

2. **Run tests with master URL**:
   ```bash
   export SPARK_MASTER_URL="spark://localhost:7077"
   pytest tests/test_inc_join.py
   ```

### Option 3: Local Mode (Default - Recommended for pip-installed PySpark)

**This is the easiest option and works with pip-installed PySpark!**

If no environment variables are set, tests will use local mode. Each test will create its own local Spark session:

**Windows:**
```powershell
# PowerShell - Just run tests, no setup needed!
pytest tests/test_inc_join.py

# Command Prompt
pytest tests/test_inc_join.py
```

**Linux/Mac:**
```bash
pytest tests/test_inc_join.py
```

> **Note**: 
> - No external server setup needed
> - Works with pip-installed PySpark
> - Each test creates its own Spark session
> - Spark sessions are automatically cleaned up after tests
> - This is the **recommended approach** for most development scenarios

## Environment Variables

- `SPARK_CONNECT_URL`: Connect to Spark Connect server (e.g., `sc://localhost:15002`)
- `SPARK_MASTER_URL`: Connect to Spark Standalone master (e.g., `spark://localhost:7077`)

## Troubleshooting

### "ClassNotFoundException: org.apache.spark.sql.connect.service.SparkConnectServer"

This error occurs when you try to use `spark-class.cmd` but PySpark was installed via pip (which doesn't include the full Spark distribution).

**Why this happens:**
- Pip-installed PySpark only includes the Python API, not the full Spark distribution
- Spark Connect server requires the full Spark installation with Java classes

**Solutions:**
1. **Use Option 3 (Local mode) - Easiest**: Just run `pytest tests/test_inc_join.py` without any environment variables
2. **Download full Spark**: Download Spark from https://spark.apache.org/downloads.html, set SPARK_HOME, then use Option 1 or 2
3. **Use existing Spark cluster**: If you have access to a Spark cluster, use Option 1 or 2 with the cluster URL

### "Cannot connect to Spark server"

1. **Check if server is running**:
   ```powershell
   # Windows PowerShell - For Spark Connect
   Invoke-WebRequest -Uri http://localhost:15002/version
   
   # Windows Command Prompt - For Spark Connect
   curl http://localhost:15002/version
   
   # For Spark Standalone - check web UI
   # Open http://localhost:8080 in browser
   ```
   
   ```bash
   # Linux/Mac - For Spark Connect
   curl http://localhost:15002/version
   
   # For Spark Standalone - check web UI
   # Open http://localhost:8080 in browser
   ```

2. **Check firewall/network**: Ensure the port is accessible

3. **Check hostname**: Use `localhost` for local servers, or the correct hostname for remote servers

4. **Check Spark version**: Spark Connect requires Spark 3.4+

### "Port already in use"

If port 15002 (Spark Connect) or 7077 (Standalone) is already in use:

**Windows:**
- Find and stop the existing process:
  ```powershell
  # Find process using the port
  netstat -ano | findstr :15002
  # Stop the process (replace PID with actual process ID)
  taskkill /PID <PID> /F
  ```
- Or use a different port:
  ```powershell
  # Start server on different port
  cd $env:SPARK_HOME
  .\bin\spark-class.cmd org.apache.spark.sql.connect.service.SparkConnectServer --port 15003
  ```
  Then set: `$env:SPARK_CONNECT_URL = "sc://localhost:15003"`

**Linux/Mac:**
- Find and stop the existing process:
  ```bash
  # Find process using the port
  lsof -i :15002
  # Stop the process
  kill -9 <PID>
  ```
- Or use a different port:
  ```bash
  $SPARK_HOME/sbin/start-connect-server.sh --port 15003
  ```
  Then set: `export SPARK_CONNECT_URL="sc://localhost:15003"`

## Notes

- External Spark sessions are **not stopped** by the tests (to preserve your server)
- Local Spark sessions are automatically stopped after tests complete
- The Spark Connect approach is recommended as it's more lightweight and modern

