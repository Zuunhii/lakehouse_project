import pyodbc

try:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=192.168.1.111;"
        "DATABASE=AdventureWorks2022;"
        "UID=sa;"
        "PWD=Zuunhii;"
        "TrustServerCertificate=yes"
    )
    print("Kết nối thành công!")
    conn.close()
except Exception as e:
    print("Kết nối thất bại:", e)