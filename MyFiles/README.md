MSSQL1 is designed to read from SQL Server, write locally to a CSV, and then take that CVS and put it in an S3 bucket.
(Currently experiencing a input string 60s timeout error...will work on that later)

MSSQL2 is designed to read from SQL Server, write locally to a CSV, copy that CSV to S3 then convert to a Parquet.
