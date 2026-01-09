🏗️ Lakehouse Project

Khởi Đầu với Data Warehouse (Kho Dữ Liệu)
Trong giai đoạn đầu của kỷ nguyên dữ liệu, các doanh nghiệp nhận ra rằng việc phân tích các báo cáo phức tạp trực tiếp trên cơ sở dữ liệu giao dịch hàng ngày là không khả thi. Điều này làm giảm hiệu suất hệ thống kinh doanh cốt lõi.

Vấn đề đặt ra: Cần một nơi riêng biệt, có cấu trúc để lưu trữ dữ liệu đã được làm sạch, sẵn sàng cho các phân tích chuyên sâu (OLAP).

Giải pháp: Data Warehouse (DW) ra đời. DW mang lại độ tin cậy cao nhờ khả năng thực thi cấu trúc chặt chẽ (Schema-on-Write) và đảm bảo các giao dịch ACID, lý tưởng cho Business Intelligence (BI) và báo cáo tài chính quan trọng.

Nhưng sau đó... Khi lượng dữ liệu bùng nổ, đặc biệt là các loại dữ liệu mới như log, sensor, hình ảnh, và video (dữ liệu phi cấu trúc), DW bắt đầu bộc lộ nhược điểm. Nó quá đắt đỏ, quá cứng nhắc, và gần như không thể xử lý các loại dữ liệu thô này.

Sự Trỗi Dậy của Data Lake (Hồ Dữ Liệu)
Để đối phó với sự bùng nổ dữ liệu thô và nhu cầu về một nền tảng chi phí thấp cho Machine Learning (ML) và Data Science, Data Lake xuất hiện.

Vấn đề đặt ra: Cần một nơi có khả năng lưu trữ mọi loại dữ liệu ở dạng thô với chi phí cực thấp, không giới hạn dung lượng, và linh hoạt cho các thuật toán AI/ML.

Giải pháp: Data Lake (DL) được xây dựng trên nền tảng lưu trữ đối tượng (ví dụ: S3, ADLS). DL cho phép chúng ta lưu trữ dữ liệu với nguyên tắc Schema-on-Read (định nghĩa cấu trúc khi đọc), mang lại sự linh hoạt tuyệt đối.

Tuy nhiên, đây là rào cản... Sự linh hoạt đó phải trả giá bằng độ tin cậy. Data Lake thiếu các tính năng quản lý giao dịch (ACID), dẫn đến vấn đề về chất lượng và tính nhất quán của dữ liệu. Việc thực hiện các báo cáo BI nghiêm ngặt trên Data Lake trở nên rủi ro và khó khăn, thường biến nó thành "Data Swamp" (Đầm lầy dữ liệu).

Nhu Cầu Hợp Nhất và Sự Ra Đời của Lakehouse
Các tổ chức thấy mình bị kẹt trong việc phải duy trì cả hai hệ thống: DW cho BI đáng tin cậy và DL cho AI/ML linh hoạt.

Vấn đề đặt ra: Việc duy trì hai hệ thống song song tạo ra sự phức tạp, trùng lặp dữ liệu, và độ trễ cao do phải di chuyển dữ liệu liên tục giữa hai nơi. Chi phí vận hành tăng lên, và việc tạo ra một nguồn dữ liệu chân thật duy nhất (Single Source of Truth) trở nên bất khả thi.

Giải pháp đột phá: Kiến trúc Data Lakehouse ra đời để giải quyết triệt để vấn đề này.

Lakehouse là sự kết hợp tối ưu: Nó tận dụng chi phí thấp và khả năng mở rộng của Data Lake, nhưng bổ sung một lớp quản lý giao dịch (Transaction Layer) (ví dụ: Delta Lake, Iceberg) để mang lại tính nhất quán, ACID, và cấu trúc cần thiết của Data Warehouse.

Kết quả: Lakehouse cho phép chúng ta thực hiện tất cả các tác vụ (BI, AI/ML, Streaming) trên một bản sao dữ liệu duy nhất, loại bỏ sự phức tạp, giảm thiểu chi phí và tăng tốc độ phân tích cho toàn bộ doanh nghiệp.



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

Cấu trúc thư mục dự án 
```
├── containers
│   ├── airflow/           # Thư mục chứa file dockerfile của airflow
│   ├── setup/             # Thử mục setup bucket cho MiniO
│
├── dags/                  # Nơi chứa các file định nghĩa DAG
│
├── dbt_trino_project      # dbt project dùng Trino làm engine
│   ├── models             # dbt models (stagging, intermediate, marts)
│   ├── macros             # dbt macros tái sử dụng
│   ├── dbt_project.yml    # dbt project configuration
│   └── profiles.yml       # File này định nghĩa thông tin catalog, schema, user và endpoint của Trino
│
├── data_detail/            # text chú thích
│
├── conf/                   # Thư mục chứa file cấu hình của hive metastore
├── etc/                    # Các file cấu hình trino
│   └── catalog/            # Cấu hình iceberg
├── SCRIPTS/                # Các Script python
│
├── docker-compose.yml      # file docker-compose chính của dự án
├── Makefile                
├── requirements.txt        # file yêu cầu các thư viện
```



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





