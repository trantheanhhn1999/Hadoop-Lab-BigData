# BÁO CÁO THỰC HÀNH HADOOP STREAMING TRÊN GOOGLE COLAB
**Chủ đề:** Phân tích doanh thu đơn hàng thương mại điện tử bằng mô hình MapReduce

---

## 1. Mục tiêu và Dữ liệu đầu vào

### 1.1. Mục tiêu bài thực hành
* **Về kỹ thuật:** Triển khai Apache Hadoop (phiên bản 3.5.0) trên môi trường đám mây Google Colab, xây dựng chương trình xử lý dữ liệu phân tán theo mô hình MapReduce bằng ngôn ngữ Python và thực thi thông qua công cụ Hadoop Streaming ở chế độ Local.
* **Về nghiệp vụ:** Xử lý tập dữ liệu đơn hàng thương mại điện tử để tổng hợp doanh thu theo từng danh mục sản phẩm (Category), từ đó xác định Top 3 nhóm ngành hàng mang lại nguồn thu lớn nhất (chỉ ghi nhận các giao dịch thành công).

### 1.2. Dữ liệu đầu vào
* **Tên tệp dữ liệu:** `orders_2026_09.csv`
* **Cấu trúc dữ liệu:** Tệp định dạng CSV chứa lịch sử giao dịch đơn hàng, bao gồm các trường thông tin:
  * `transaction_id`: Mã giao dịch
  * `order_date`, `order_time`: Ngày và giờ đặt hàng
  * `province`: Tỉnh/Thành phố giao hàng
  * `category`: Nhóm sản phẩm (Electronics, Home, Fashion, Sport, Beauty, Toy, Grocery, Book)
  * `channel`: Kênh bán hàng (Web, App)
  * `quantity`, `unit_price`, `amount`: Số lượng, đơn giá và tổng thành tiền của đơn hàng
  * `status`: Trạng thái đơn hàng (`SUCCESS` hoặc `FAILED`)
  * `shipping_days`: Số ngày vận chuyển

---

## 2. Giải thích luồng xử lý dữ liệu (MapReduce)

Quá trình phân tích dữ liệu được chia thành các giai đoạn tuần tự thông qua cơ chế Hadoop Streaming:

1. **Giai đoạn Map (`mapper_revenue.py`):**
   * Chương trình đọc luồng dữ liệu đầu vào theo từng dòng từ tệp `orders_2026_09.csv` thông qua `sys.stdin`.
   * Thực hiện kiểm tra điều kiện lọc: Chỉ giữ lại các bản ghi có trạng thái giao dịch thành công (`status == "SUCCESS"`), loại bỏ hoàn toàn các giao dịch thất bại (`FAILED`).
   * Phát sinh đầu ra là các cặp khóa - giá trị (Key-Value) phân cách bởi ký tự Tab (`\t`) theo cấu trúc: `category \t amount` (Ví dụ: `Electronics    3400000`).

2. **Giai đoạn Shuffle & Sort (Hadoop tự động xử lý):**
   * Hệ thống Hadoop thu thập toàn bộ đầu ra từ Mapper, tự động sắp xếp và gom nhóm các bản ghi có cùng khóa (`category`) lại gần nhau trước khi chuyển sang cho Reducer.

3. **Giai đoạn Reduce (`reducer_revenue.py`):**
   * Chương trình nhận các dòng dữ liệu đã được sắp xếp theo thứ tự bảng chữ cái của `category`.
   * Thực hiện cộng dồn giá trị doanh thu (`amount`) cho từng nhóm sản phẩm tương ứng.
   * Ghi kết quả tổng hợp cuối cùng ra tệp đầu ra `part-00000` trong thư mục `/content/output_revenue`.

---

## 3. Kết quả thực nghiệm

### 3.1. Toàn bộ kết quả đầu ra từ Hadoop (`part-00000`)
Dưới đây là toàn bộ dữ liệu doanh thu của 8 nhóm ngành hàng sau khi chạy thành công công việc MapReduce:

```text
Beauty          111032003
Book            39306022
Electronics     3703769311
Fashion         218597763
Grocery         80983147
Home            410315065
Sport           166194354
Toy             83881779
```

### 3.2. Bảng tổng hợp toàn bộ các nhóm ngành hàng
*(Sắp xếp theo thứ tự doanh thu từ cao xuống thấp)*

| Thứ hạng | Nhóm sản phẩm (Category) | Tổng doanh thu (VNĐ) | Tỷ trọng (%) |
| :---: | :--- | ---: | :---: |
| **1** | **Electronics** | **3.703.769.311** | **76,94%** |
| **2** | **Home** | **410.315.065** | **8,52%** |
| **3** | **Fashion** | **218.597.763** | **4,54%** |
| 4 | Sport | 166.194.354 | 3,45% |
| 5 | Beauty | 111.032.003 | 2,31% |
| 6 | Toy | 83.881.779 | 1,74% |
| 7 | Grocery | 80.983.147 | 1,68% |
| 8 | Book | 39.306.022 | 0,82% |
| **Tổng cộng** | | **4.814.079.444** | **100,00%** |

### 3.3. Bảng kết quả Top 3 nhóm sản phẩm có doanh thu cao nhất

| Hạng | Category | Doanh thu (VNĐ) | Nhận xét |
| :---: | :--- | ---: | :--- |
| **1** | **Electronics** | 3.703.769.311 | Nhóm có doanh thu cao nhất, giữ vai trò chủ lực và áp đảo hoàn toàn các nhóm còn lại (chiếm gần 77% tổng doanh thu). |
| **2** | **Home** | 410.315.065 | Nhóm có doanh thu đứng thứ hai, đóng góp nguồn thu ổn định cho sàn thương mại điện tử. |
| **3** | **Fashion** | 218.597.763 | Nhóm có doanh thu đứng thứ ba, có tiềm năng phát triển tốt bên cạnh nhóm hàng gia dụng. |

---

## 4. Đánh giá và Ý nghĩa nghiệp vụ

Từ kết quả phân tích dữ liệu thông qua Hadoop Streaming, doanh nghiệp có thể rút ra các định hướng quản trị quan trọng sau:

* **Tập trung nguồn lực cho ngành hàng mũi nhọn (Electronics):** Với doanh thu đạt hơn 3,7 tỷ VNĐ, ngành hàng Điện tử là nguồn sống chính của hệ thống. Doanh nghiệp cần ưu tiên tối đa cho nhóm này trong việc đảm bảo nguồn cung tồn kho không bị đứt gãy, kiểm soát chất lượng đóng gói, tối ưu tốc độ giao hàng và đầu tư dịch vụ chăm sóc khách hàng/bảo hành hậu mãi.
* **Thúc đẩy các ngành hàng tiềm năng (Home & Fashion):** Hai nhóm Gia dụng (hơn 410 triệu VNĐ) và Thời trang (hơn 218 triệu VNĐ) đứng ở vị trí thứ 2 và thứ 3 nhưng khoảng cách với nhóm dẫn đầu còn rất xa. Có thể triển khai các chiến dịch bán chéo (Cross-selling), tung mã giảm giá hoặc chương trình khuyến mại theo combo để kích cầu hai nhóm ngành hàng này.
* **Tối ưu hóa các nhóm doanh thu thấp (Book, Grocery, Toy):** Cần rà soát lại danh mục sản phẩm, chi phí lưu kho và chiến lược tiếp thị đối với nhóm Sách, Bách hóa và Đồ chơi để cải thiện biên lợi nhuận hoặc cơ cấu lại danh mục hàng hóa.

---

