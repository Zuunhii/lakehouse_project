import pandas as pd
import boto3
from io import BytesIO
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# Cấu hình kết nối đến MinIO (hoặc S3)
MINIO_ENDPOINT = "http://localhost:9000"  # Sử dụng MinIO địa chỉ nội bộ
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "lakehouse"

# Đọc dữ liệu từ MinIO (hoặc S3) - Tải Parquet từ MinIO
def load_parquet_from_minio(object_key):
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    # Tải dữ liệu từ MinIO
    obj = s3.get_object(Bucket=BUCKET, Key=object_key)
    
    # Đọc dữ liệu Parquet vào pandas DataFrame
    buffer = BytesIO(obj['Body'].read())
    df = pd.read_parquet(buffer)
    
    return df

# Đọc dữ liệu từ bảng kết hợp (dựa trên object_key)
object_key = 'silver/ml_combined_customer_sales-919d80ede4cd4ece87a69beff14755aa/data/20251203_150829_00247_5vyi4-4e910842-02d9-4a8c-b61c-fcb8a01f4e41.parquet'  # Sửa theo đường dẫn thực tế
df = load_parquet_from_minio(object_key)

# Kiểm tra tên cột trong DataFrame
print(df.columns)

# Tính toán các cột cần thiết nếu chưa có

# 1. Tổng chi tiêu của khách hàng (total_spent)
if 'total_spent' not in df.columns:
    df['total_spent'] = df.groupby('customerkey_wid')['revenue'].transform('sum')

# 2. Số lượng đơn hàng của khách hàng (order_count)
if 'order_count' not in df.columns:
    df['order_count'] = df.groupby('customerkey_wid')['sales_order_id'].transform('count')

# 3. Giá trị trung bình mỗi đơn hàng (avg_order_value)
if 'avg_order_value' not in df.columns:
    df['avg_order_value'] = df['total_spent'] / df['order_count']

# 4. Ngày mua hàng gần nhất (last_purchase_date)
if 'last_purchase_date' not in df.columns:
    df['last_purchase_date'] = df.groupby('customerkey_wid')['orderdate'].transform('max')

# 5. Tạo cột churn (Giả sử churn = 1 nếu total_spent < 1000, ngược lại = 0)
if 'churn' not in df.columns:
    df['churn'] = (df['total_spent'] < 1000).astype(int)

# Kiểm tra missing values
print(df.isnull().sum())

# Lựa chọn các tính năng (features) và mục tiêu (target)
X = df[['total_spent', 'order_count', 'avg_order_value', 'last_purchase_date']]  # Các tính năng
y = df['churn']  # Mục tiêu phân loại: churn (1 nếu khách hàng có khả năng bỏ đi, 0 nếu không)

# Chuyển đổi ngày thành số (số ngày kể từ một ngày tham chiếu, ví dụ như ngày đầu tiên trong dữ liệu)
X['last_purchase_date'] = (pd.to_datetime('today') - pd.to_datetime(X['last_purchase_date'])).dt.days

# Chuẩn hóa dữ liệu (standardization)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Chia dữ liệu thành tập huấn luyện và kiểm thử
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# Kiểm tra kích thước tập huấn luyện và kiểm thử
print(f"Tập huấn luyện: {X_train.shape}, Tập kiểm thử: {X_test.shape}")

# Huấn luyện mô hình Random Forest
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Dự đoán và đánh giá mô hình
y_pred = model.predict(X_test)

# Đánh giá mô hình
print(classification_report(y_test, y_pred))
