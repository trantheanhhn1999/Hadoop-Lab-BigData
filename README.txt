Bộ dữ liệu mẫu cho bài thực hành Hadoop online

File chính: orders_2026_09.csv
Bài toán: phân tích đơn hàng thương mại điện tử để tính doanh thu theo nhóm sản phẩm bằng Hadoop Streaming MapReduce.

Các file script:
- mapper_revenue.py: phát sinh cặp category\tamount cho các giao dịch SUCCESS.
- reducer_revenue.py: cộng doanh thu theo category.
