@echo off

echo 🟡 Starting incremental ETL...

cd C:\Users\Alen\PycharmProjects\PythonProject\data-analytics
call ..\.venv\Scripts\activate.bat
python orchestration\run_etl_incremental.py

echo ✅ ETL finished.

pause
