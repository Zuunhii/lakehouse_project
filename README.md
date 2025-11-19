🏗️ Lakehouse Project

📖 Bối Cảnh: Sự Tiến Hóa của Kiến Trúc Dữ Liệu
Kiến trúc Lakehouse ra đời để giải quyết những hạn chế cố hữu của các mô hình lưu trữ dữ liệu truyền thống, đặc biệt là sự phân tách giữa Kho Dữ Liệu (Warehouse) và Hồ Dữ Liệu (Data Lake).

1. Kỷ Nguyên Data Warehouse (DW)
Mục tiêu: Phục vụ các ứng dụng Báo cáo và Phân tích Kinh doanh (BI) truyền thống.

Đặc điểm: Dữ liệu có cấu trúc cao (Schema-on-Write), cung cấp Độ tin cậy và Tính nhất quán (ACID).

Hạn chế: Chi phí cao, không linh hoạt, khó xử lý Dữ liệu phi cấu trúc (Unstructured Data) và Dữ liệu thô.

2. Kỷ Nguyên Data Lake (DL)
Mục tiêu: Lưu trữ tất cả các loại dữ liệu (thô, phi cấu trúc) với chi phí thấp để phục vụ Data Science và Machine Learning (ML).

Đặc điểm: Dữ liệu được lưu trữ nguyên trạng (Schema-on-Read), linh hoạt và mở rộng vô hạn.

Hạn chế: Thiếu ACID, khó đảm bảo Chất lượng dữ liệu (dẫn đến "Data Swamp"), không đủ độ tin cậy cho các tác vụ BI quan trọng.

3. Vấn Đề Cốt Lõi (The Data Gap)
Việc duy trì hai hệ thống song song (Warehouse cho BI và Lake cho ML) dẫn đến:

Phân mảnh Dữ liệu (Data Silos): Dữ liệu bị trùng lặp, phải di chuyển liên tục, gây ra độ trễ (latency) và tốn kém chi phí ETL/ELT.

Phức tạp trong Quản trị: Yêu cầu quản lý và bảo mật trên hai nền tảng khác nhau.

🌟 Giải Pháp: Data Lakehouse
Lakehouse là một kiến trúc thống nhất, kết hợp những ưu điểm tốt nhất của Data Warehouse và Data Lake trên cùng một nền tảng lưu trữ dữ liệu mở và chi phí thấp.



<img width="1022" height="603" alt="image" src="https://github.com/user-attachments/assets/ab61e5a0-1813-42ac-9c7a-7a6783bae167" />

Mô hình tổ chức dữ liệu theo ba tầng:
Bronze – dữ liệu thô, nạp từ nguồn gốc.
Silver – dữ liệu đã được làm sạch và chuẩn hóa schema.
Gold – dữ liệu tổng hợp, phục vụ phân tích và dashboard.


⚙️ Công nghệ sử dụng
| Thành phần               | Vai trò            | Mô tả                                                                                                   |
| ------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------- |
| **MinIO**                | Data Lake          | Object storage tương thích S3, lưu dữ liệu ở các tầng bronze/silver/gold.                               |
| **Apache Iceberg**       | Table Format       | Quản lý schema, versioning, partition và ACID transaction trên data lake.                               |
| **Trino**                | Query Engine       | Xử lý truy vấn SQL phân tán, đọc bảng Iceberg thông qua Hive Metastore và MinIO.                        |
| **Hive Metastore (HMS)** | Metadata Catalog   | Lưu trữ metadata (schema, partition, snapshot) cho các bảng Iceberg, làm cầu nối giữa Trino và Iceberg. |
| **Airflow**              | Orchestration      | Điều phối và tự động hóa pipeline ETL/ELT.                                                              |
| **DBT (optional)**       | Transformation     | Quản lý logic biến đổi dữ liệu SQL ở tầng Silver/Gold, dùng Trino làm engine.                           |
| **Python utilities**     | Tooling            | Hỗ trợ đọc/ghi file, đăng ký Iceberg schema, chuyển đổi dữ liệu giữa các tầng.                          |
| **Metabase (optional)**  | BI / Visualization | Kết nối với Trino để trực quan hóa dữ liệu qua dashboard, chart và báo cáo.                             |




📦 Dữ liệu sử dụng: AdventureWorks

Dự án sử dụng AdventureWorks, bộ dữ liệu mẫu nổi tiếng của Microsoft mô phỏng hoạt động của một công ty sản xuất và bán hàng toàn cầu.
AdventureWorks chứa dữ liệu đa dạng, phù hợp cho cả kỹ thuật Data Modeling, ETL, và BI:

Production – sản phẩm, danh mục, tồn kho, quy trình sản xuất.

Sales – khách hàng, đơn hàng, thanh toán, thẻ tín dụng.

Person – thông tin nhân viên, địa chỉ, quốc gia, vùng địa lý.

HumanResources – dữ liệu nhân sự, phòng ban, chức vụ.

Purchasing – nhà cung cấp, phiếu mua hàng.

Trong dự án này:

Dữ liệu raw CSV/Parquet được lưu ở tầng Bronze (trên MinIO).

Sau đó được chuẩn hóa và tách quan hệ thành các bảng Silver (Iceberg).

Cuối cùng, dữ liệu được tổng hợp và tính toán KPI ở tầng Gold phục vụ báo cáo.



Bronze - dữ liệu raw được lưu trữ ở trên MiniO 

<img width="601" height="284" alt="image" src="https://github.com/user-attachments/assets/58dc091d-6309-454e-9527-8a3ff0021076" />

Silver - đăng ký bảng và xử lý, làm sạch dữ liệu 

<img width="574" height="393" alt="image" src="https://github.com/user-attachments/assets/66db2bc7-4feb-4f70-ba90-df8152f5c01c" />

Gold – dữ liệu tổng hợp, phục vụ phân tích và dashboard.
sử dụng dbt và airflow để modeling các bảng dữ liệu về dim và fact và làm dashboard cơ bản 

<img width="582" height="191" alt="image" src="https://github.com/user-attachments/assets/5f25ef4c-839a-4701-bd69-0c95ea5dc90b" />

<img width="584" height="464" alt="image" src="https://github.com/user-attachments/assets/f28a85b1-7451-4cf5-bac9-b6dc25c003dc" />





