
---
# OVERVIEW.md

# NYC TRANSIT REAL-TIME PASSENGER FLOW & OVERCROWDING ANALYTICS SYSTEM
> **Hệ thống phân tích lưu lượng hành khách và cảnh báo quá tải nhà ga thời gian thực cho mạng lưới giao thông công cộng New York dựa trên kiến trúc Lambda**

---

## 1. TỔNG QUAN DỰ ÁN (EXECUTIVE SUMMARY)

* **Môn học:** Lưu trữ và Xử lý Dữ liệu lớn (Big Data Storage & Processing) - SOICT, HUST.
* **Lĩnh vực ứng dụng:** Đô thị thông minh (Smart City) & Giao thông công cộng (Intelligent Transit).
* **Mục tiêu cốt lõi:** 
  Xây dựng một hệ sinh thái dữ liệu phân tán khép kín từ khâu thu thập, lưu trữ, tính toán song song (Lô & Luồng) đến trực quan hóa nhằm:
  1. **Theo dõi dòng chảy hành khách tức thời (Real-time Stream):** Giám sát lưu lượng vào/ra tại gần 500 nhà ga tàu điện ngầm New York, tự động phát hiện và cảnh báo sớm các điểm nóng quá tải nguy hiểm (Overcrowding Anomaly Alert) với độ trễ dưới 3 giây.
  2. **Khai phá quy luật giao thông lịch sử (Batch Processing):** Lưu trữ phân tán nhiều tháng dữ liệu cảm biến, phân cụm hành vi các nhà ga bằng học máy phân tán (**Spark MLlib K-Means**) và đánh giá tương quan giữa hiện tượng quá tải với chỉ số chậm giờ tàu (**MTA Wait Assessment**).

---

## 2. DỮ LIỆU ĐẦU VÀO (INPUT DATA PROFILE)

Dữ liệu được trích xuất từ cổng thông tin chính thống của **Metropolitan Transportation Authority (MTA)** và **NYC Open Data**, được lưu trữ trong thư mục `data/archive/`:

```
data/archive/
├── MTA_StationLocations.csv         # Danh mục chuẩn 493 ga tàu điện ngầm NYC (Station ID, Complex ID, Coordinates)
├── TurnstileData/
│   └── turnstile_*.txt              # 27 tệp chuỗi thời gian trọn vẹn 6 tháng (160102 -> 160702, ~656 MB, 5.2M dòng)
├── NYC_TransitSubway/               # 9 tệp chuẩn GTFS (routes.txt, trips.txt, stop_times.txt, stops.txt, shapes.txt...)
└── NYC_TransitPerformance/          # 14 tệp KPI điều hành (MTA_Performance_Agencies.csv, NYCT.csv, Metadata...)
```

### 2.1. Chi tiết các tệp dữ liệu chính & Khảo sát thực nghiệm

| Tệp dữ liệu | Bản chất dữ liệu | Dung lượng / Quy mô thực tế | Các trường thông tin cốt lõi (Schema) | Đặc thù kỹ thuật phát hiện qua khảo sát |
| :--- | :--- | :--- | :--- | :--- |
| **`turnstile_*.txt` / `turnstile_*.csv`** | Chuỗi thời gian cảm biến công tơ mét (Cumulative Sensor Logs) | 27 tệp tuần (656 MB thô), **5.183.000 dòng**, 4.567 turnstile vật lý | `C/A`, `UNIT`, `SCP`, `STATION`, `LINENAME`, `DIVISION`, `DATE`, `TIME`, `DESC`, `ENTRIES`, `EXITS` | • Chu kỳ ghi nhận cảm biến: xấp xỉ **4 giờ/lần** (03:00, 07:00, 11:00, 15:00, 19:00, 23:00 hoặc 00:00, 04:00, ...).<br>• Cột header `EXITS` có khoảng trắng thừa cuối dòng (`EXITS    ...`).<br>• Xuất hiện hiện tượng reset bộ đếm âm (~1.250 lỗi/tuần) và xung số ảo (>7.200/khoảng). |
| **`MTA_StationLocations.csv`** | Danh mục chuẩn không gian (Geospatial Subway Index) | 493 nhà ga/tổ hợp ga thuộc 5 quận New York | `Station ID`, `Complex ID`, `GTFS Stop ID`, `Division`, `Line`, `Stop Name`, `Borough`, `Daytime Routes`, `GTFS Latitude`, `GTFS Longitude` | • Bảng liên kết không gian chuẩn xác nhất cho ga ngầm NYC.<br>• Phân bố theo quận: Brooklyn (169), Manhattan (151), Queens (82), Bronx (70), Staten Island (21). |
| **`stops.csv` (GTFS)** | Dữ liệu dừng đỗ GTFS | 1.503 bản ghi (501 điểm dừng, 380 ga cha) | `stop_id`, `stop_name`, `stop_lat`, `stop_lon`, `location_type`, `parent_station` | • Cần lọc `location_type = '1'` để lấy danh mục ga cha (Parent Stations). |
| **`MTA_Performance_Agencies.csv`** | Bảng chỉ số điều hành hàng tháng (Performance KPIs) | 57.45 MB, 13.862 dòng, 17 cột (2008 – 2017) | `AGENCY_NAME`, `INDICATOR_NAME`, `PERIOD_YEAR`, `PERIOD_MONTH`, `MONTHLY_ACTUAL`, `MONTHLY_TARGET`, `YYYY_MM` | • Mã hóa tệp: **`ISO-8859-1` / `Windows-1252`** (chứa ký tự non-breaking space `0xA0`). Cần đọc đúng encoding.<br>• Có 288 bản ghi năm 2016 về **`Subway Wait Assessment`** và **`OTP (Terminal)`** chia theo từng tuyến (1-7, A-R). |

### 2.2. Kiểm chứng đặc trưng 3V của Big Data
* **Volume:** Hơn 5,18 triệu bản ghi sự kiện cảm biến turnstile kết hợp dữ liệu bảng KPI và đặc tả GTFS, kích thước vượt quá giới hạn xử lý in-memory hiệu năng cao trên máy đơn lẻ mà không áp dụng mô hình phân tán.
* **Velocity:** Tái hiện tốc độ phát sinh sự kiện quẹt thẻ thực tế của hành khách New York thông qua cơ chế **Python Event Simulator & Flow Interpolation**, đẩy vào Kafka đạt thông lượng **2.000 – 5.000 events/giây**.
* **Variety:** Đa dạng thể loại: Dữ liệu cảm biến lũy tiến (CSV/TXT), Dữ liệu GIS tọa độ địa lý (`MTA_StationLocations.csv`, `stops.csv`), Bảng chỉ số vận hành (CSV `ISO-8859-1`), Dữ liệu bán cấu trúc XML (`Performance_NYCT.xml`) và GTFS transit schedules.

---

## 3. KIẾN TRÚC KỸ THUẬT & XỬ LÝ DỰ KIẾN (SYSTEM ARCHITECTURE)

Hệ thống được thiết kế hoàn chỉnh theo **Kiến trúc Lambda (Lambda Architecture)**:

```
[Dữ liệu thô CSV/Logs] 
        │
        ▼
[Python Streaming Producer]
        │ (Kafka Message Protocol)
        ▼
[Apache Kafka Broker] (Topic: mta-turnstile-stream)
        ├───────────────────────────────────────────────────────┐
        │ (Micro-batch Ingestion)                               │ (Real-time Stream)
        ▼                                                       ▼
┌────────────────────────────────────────┐   ┌────────────────────────────────────────┐
│              BATCH LAYER               │   │              SPEED LAYER               │
│ • Storage: Hadoop HDFS (Parquet)       │   │ • Engine: Spark Structured Streaming   │
│ • Engine: Apache Spark Batch           │   │ • Window: 15 phút, trượt 1 phút        │
│ • Logic:                               │   │ • Logic:                               │
│   - Giải thuật xử lý tràn bộ đếm       │   │   - Tính Flow Rate tức thời            │
│   - Spatial Join Station Locations     │   │   - Thuật toán Z-Score Anomaly         │
│   - Spark MLlib K-Means gom cụm trạm   │   │   - Kích hoạt Overcrowding Alert       │
└────────────────────────────────────────┘   └────────────────────────────────────────┘
        │ (Batch Views)                                         │ (Real-time Views)
        └───────────────────────────────┬───────────────────────┘
                                        ▼
                        ┌────────────────────────────────┐
                        │         SERVING LAYER          │
                        │ • Elasticsearch Cluster        │
                        │ • Geo-Point Indexing           │
                        └────────────────────────────────┘
                                        │
                                        ▼
                        ┌────────────────────────────────┐
                        │    PRESENTATION LAYER (UI)     │
                        │ • Kibana Live Heatmap          │
                        │ • Grafana Metrics & Alerts     │
                        └────────────────────────────────┘
```

### 3.1. Tầng Thu thập (Ingestion Layer)
* **Thành phần:** `Python Event Simulator` + **Apache Kafka**.
* **Cơ chế phát sinh luồng từ nhật ký chu kỳ 4 giờ:**
  Do bản chất dữ liệu turnstile gốc được chốt số lũy kế theo chu kỳ xấp xỉ 4 giờ/lần (thay vì từng giây), bộ giả lập `Python Event Simulator` triển khai 2 chế độ:
  1. *Chế độ 1 - Micro-event Flow Interpolator (Khuyến nghị cho Streaming):* Đọc các cặp bản ghi liên tiếp để tính chênh lệch $\Delta$, sau đó áp dụng hàm phân phối Poisson hoặc tỷ trọng giờ cao điểm trong ngày để phân rã lượng khách thành các micro-events/dòng lưu lượng 1 phút, đẩy vào Kafka với thông lượng **2.000 – 5.000 messages/giây**.
  2. *Chế độ 2 - Accelerated Snapshot Replay:* Phát lại tuần tự chuỗi sự kiện lịch sử theo trục thời gian được nén gia tốc (ví dụ 1 giây thực = 1 giờ mô phỏng) để kiểm thử sức chịu tải tối đa của hệ thống.

### 3.2. Tầng Xử lý Lô (Batch Layer)
* **Lưu trữ:** Dữ liệu thô sau khi tiền xử lý được ghi lên **HDFS** dưới định dạng tối ưu **Parquet** (nén Snappy), phân vùng (partitioned) theo `year_month` và `station_id`.
* **Xử lý phân tán (PySpark):**
  1. **Làm sạch bộ đếm lũy kế:** 
     * Gom nhóm theo định danh turnstile vật lý `(C/A, UNIT, SCP, STATION)`.
     * Dùng window function `lag()` sắp xếp theo thời gian để tính:
       $$\Delta Entries = Entries_t - Entries_{t-1}, \quad \Delta Exits = Exits_t - Exits_{t-1}$$
     * **Bộ lọc dị thường:**
       - Lọc bỏ hoặc gán giá trị hợp lệ khi $\Delta < 0$ (máy reset bộ đếm, bảo trì hoặc counter quay ngược; chiếm ~1.700 trường hợp/tuần).
       - Lọc bỏ xung số ảo khi $\Delta > 7.200 \times \Delta t_{hours}$ (vượt ngưỡng vật lý tối đa 2 lượt quẹt/giây/cửa xoay).
  2. **Chuẩn hóa thực thể trạm (Entity Resolution) & Ghép nối không gian:**
     * Áp dụng từ điển đối chiếu (Crosswalk Dictionary) kết hợp Regex chuẩn hóa tên trạm Turnstile (viết tắt hoa, ví dụ `GRD CNTRL-42 ST`, `34 ST-PENN STA`) sang `Station ID / Complex ID` trong `MTA_StationLocations.csv` (`Grand Central - 42 St`, `34 St - Penn Station`).
     * Lọc bỏ các trạm không thuộc mạng lưới MTA Subway (như các ga tàu PATH: `EXCHANGE PLACE`, `PAVONIA/NEWPORT`).
     * Thực hiện Spatial Join để bổ sung kinh độ, vĩ độ, quận (`Borough`) và các tuyến phục vụ (`Daytime Routes`).
  3. **Học máy phân tán (Spark MLlib K-Means):**
     * Trích xuất vector đặc trưng 24 chiều biểu thị tỷ trọng lưu lượng hành khách theo 24 giờ trong ngày của từng nhà ga.
     * Áp dụng thuật toán K-Means phân cụm ~470 trạm ga metro New York thành các nhóm hình mẫu (Archetypes): *Ga trung tâm tài chính/hành chính (đỉnh nhọn 8h-9h và 17h-18h), Ga du lịch/thương mại (đông liên tục cả ngày), Ga dân cư vệ tinh (chiều đi sáng sớm, chiều về tối).*
  4. **Phân tích tương quan chất lượng vận hành:**
     * Kết nối dữ liệu lưu lượng tổng hợp theo tháng của từng tuyến metro với chỉ số trễ giờ tàu **`Subway Wait Assessment`** và **`OTP (Terminal)`** trong tệp `MTA_Performance_Agencies.csv` (năm 2016) để xác định định lượng ảnh hưởng của quá tải trạm đến sự chậm trễ của đoàn tàu.

### 3.3. Tầng Xử lý Luồng (Speed Layer)
* **Công nghệ:** **Spark Structured Streaming**.
* **Xử lý:**
  1. **Cửa sổ trượt (Sliding Window):** Kích thước 15 phút, chu kỳ trượt 1 phút (hoặc theo cấu hình mô phỏng), liên tục tổng hợp lưu lượng vào/ra tức thời ($FlowRate$) cho từng nhà ga.
  2. **Phát hiện bất thường quá tải (Overcrowding Anomaly Detection):** So sánh $FlowRate$ thời gian thực với ngưỡng kỳ vọng lịch sử ($Mean$) và độ lệch chuẩn ($Std$) tương ứng theo khung giờ/ngày trong tuần lấy từ Batch View. 
     $$\text{Z-Score} = \frac{FlowRate - Mean}{Std}$$
     Nếu $\text{Z-Score} > 3.0$, hệ thống lập tức kích hoạt sự kiện cảnh báo `CRITICAL_OVERCROWDING_ALERT`.

### 3.4. Tầng Phục vụ truy vấn & Trực quan hóa (Serving & Visualization Layer)
* **Serving Store:** **Elasticsearch** lưu trữ hợp nhất kết quả từ Batch Layer và Speed Layer; cấu hình trường `location` dưới dạng `geo_point` để hỗ trợ truy vấn không gian.
* **Dashboard:** **Kibana / Grafana**:
  * **Bản đồ nhiệt thời gian thực (Live Geo Heatmap):** Bản đồ số toàn bộ mạng lưới metro New York. Mỗi nhà ga là một điểm phát sáng đổi màu theo trạng thái (Xanh lá: Thông thoáng, Vàng: Đông đúc, Đỏ nhấp nháy: Quá tải nghiêm trọng).
  * **Bảng điều khiển khẩn cấp:** Danh sách Top 5 nhà ga có nguy cơ dồn ứ cao nhất cần can thiệp điều tiết tàu.

---

## 4. DỮ LIỆU ĐẦU RA (OUTPUT DELIVERABLES)

| Đầu ra | Định dạng | Tầng phục vụ | Mô tả giá trị mang lại |
| :--- | :---: | :---: | :--- |
| **Turnstile Delta Parquet** | Parquet file | HDFS / Batch | Dữ liệu sạch đã bóc tách lưu lượng thực tế từng giờ, tối ưu dung lượng nén 70% so với CSV. |
| **K-Means Station Clusters** | JSON / Table | Elasticsearch | Danh mục phân loại 470 nhà ga theo nhóm hành vi, hỗ trợ quy hoạch lịch trình tàu. |
| **Real-time Congestion Alerts**| Event stream | Elasticsearch / UI | Bản tin cảnh báo tức thời gửi tới ban điều hành khi có sự cố dồn ứ tại sân ga. |
| **Live Transit Dashboard** | Interactive Web | Kibana / Grafana | Bản đồ nhiệt không gian mạng lưới metro trực quan hóa lưu lượng theo thời gian thực. |

---

## 5. CÁC THÁCH THỨC KỸ THUẬT CỐT LÕI & GIẢI PHÁP

1. **Vấn đề "Bộ đếm công tơ mét" (Cumulative Counter Rollover & Reversal):**
   * *Thách thức:* Giá trị cảm biến là lũy kế cộng dồn; khi thiết bị bảo trì, reset về 0 hoặc gặp lỗi counter quay ngược (reverse counter), giá trị chênh lệch nhảy đột ngột tạo ra số âm hoặc số dương hàng triệu đơn vị (~1.700 sự kiện dị thường/tuần được phát hiện trên mẫu khảo sát).
   * *Giải pháp:* Thiết lập giải thuật tiền xử lý trên Spark: Chỉ tính toán $\Delta$ khi $Entries_t \ge Entries_{t-1}$ và áp ngưỡng trần vật lý: $\Delta \le 7.200 \times \Delta t_{hours}$. Nếu $\Delta < 0$ hoặc vượt trần, đánh dấu bản ghi bất thường để gán giá trị trung bình trượt cục bộ hoặc loại bỏ khỏi mẫu huấn luyện.
2. **Vấn đề Lệch chuẩn định danh trạm (Entity Resolution & Multi-system Noise):**
   * *Thách thức:* Tên trạm ở file turnstile viết tắt hoa không dấu (`42 ST-GRD CNTRL`, `34 ST-PENN STA`), trong khi file tọa độ `MTA_StationLocations.csv` viết dạng đầy đủ tiêu chuẩn (`Grand Central - 42 St`, `34 St - Penn Station`). Tỷ lệ khớp chuỗi thô là **0%**; sau khi chuẩn hóa regex thông thường chỉ đạt **~70%**. Ngoài ra, dữ liệu turnstile chứa cả các trạm tàu PATH (thuộc New Jersey/Port Authority) không nằm trong hệ thống MTA Subway.
   * *Giải pháp:* Xây dựng bảng từ điển ánh xạ (Crosswalk Mapping Dictionary) kết hợp Regex chuẩn hóa tiền tố/hậu tố (`ST`, `AV`, `BLVD`, `CTR`), lọc bỏ các trạm ngoài hệ thống MTA để nâng tỷ lệ định vị không gian lên **> 98%**.
3. **Đặc thù chu kỳ lấy mẫu 4 giờ so với luồng tức thời (4-Hour Sampling vs Near-Real-time Streaming):**
   * *Thách thức:* Dữ liệu lịch sử cảm biến được chốt số mỗi 4 giờ, trong khi Speed Layer cần cửa sổ trượt 15 phút với thông lượng streaming cao (2.000 – 5.000 events/giây).
   * *Giải pháp:* Xây dựng mô-đun `Event Simulator & Flow Interpolator` trong Python Producer: Nội suy dòng khách từ khoảng 4 giờ thành các micro-events hoặc điểm lưu lượng 1-phút theo trọng số biểu đồ giờ cao điểm thực tế, đảm bảo kiểm thử chính xác và thực tế cho thuật toán Sliding Window và phát hiện dị thường Z-Score.
4. **Bảng mã tệp tin & Bất thường định dạng Header (Data Hygiene & Encoding Quirks):**
   * *Thách thức:* Tệp `MTA_Performance_Agencies.csv` sử dụng bảng mã `ISO-8859-1`/`Windows-1252` (chứa ký tự non-breaking space `0xA0`), nếu đọc ngầm định UTF-8 trong Spark/Python sẽ phát sinh lỗi `UnicodeDecodeError`. Đồng thời cột cuối `EXITS` trong tệp `turnstile_*.csv` chứa hơn 60 ký tự khoảng trắng thừa (`EXITS   ...`).
   * *Giải pháp:* Cấu hình tùy chọn encoding `ISO-8859-1` khi đọc tệp Performance; áp dụng xử lý strip whitespace cho toàn bộ danh sách tên cột trước khi ánh xạ DataFrame schema.
5. **Bài toán cân bằng giữa Độ trễ và Độ chính xác (Lambda Trade-off):**
   * *Thách thức:* Speed Layer cần phản hồi nhanh dưới 3 giây nên không thể tính toán các chỉ số thống kê phân phối trên toàn bộ lịch sử 6 tháng.
   * *Giải pháp:* Tách biệt nhiệm vụ: Batch Layer tính sẵn bảng phân phối chuẩn ($Mean, Std$) theo khung giờ và thứ trong tuần lưu vào Serving View; Speed Layer chỉ việc truy vấn các chỉ số này để tính nhanh công thức Z-Score tức thời.

---

## 6. HẠ TẦNG & MÔI TRƯỜNG TRIỂN KHAI (DOCKER SERVICES)

Toàn bộ hệ thống được container hóa thông qua Docker Compose, sẵn sàng khởi chạy với cấu hình cổng dịch vụ như sau:

| Dịch vụ (Service) | Công nghệ / Image | Cổng mạng (Port) | Chức năng trong hệ thống |
| :--- | :--- | :---: | :--- |
| **Zookeeper** | `confluentinc/cp-zookeeper:7.3.0` | `2181` | Quản lý trạng thái cụm Kafka |
| **Kafka Broker** | `confluentinc/cp-kafka:7.3.0` | `9092` | Tiếp nhận và điều phối luồng dữ liệu quẹt thẻ |
| **HDFS NameNode** | `bde2020/hadoop-namenode:2.0.0` | `9870`, `9000` | Quản lý siêu dữ liệu (metadata) hệ thống file phân tán |
| **HDFS DataNode** | `bde2020/hadoop-datanode:2.0.0` | `9864` | Lưu trữ các khối dữ liệu Parquet |
| **Spark Master** | `bitnami/spark:3.4.0` | `8080`, `7077` | Quản lý tài nguyên và điều phối tính toán phân tán |
| **Spark Worker** | `bitnami/spark:3.4.0` | `8081` | Nút thực thi tính toán Batch và Streaming |
| **Elasticsearch** | `docker.elastic.co/elasticsearch:7.17.0`| `9200` | Lưu trữ chỉ mục tìm kiếm và dữ liệu không gian Geo-Point |
| **Kibana** | `docker.elastic.co/kibana:7.17.0` | `5601` | Giao diện hiển thị Dashboard và bản đồ nhiệt giao thông |

---

## 7. CHỈ SỐ NGHIỆM THU DỰ ÁN (EVALUATION METRICS)

* **Hiệu năng hệ thống (Engineering Metrics):**
  * Thông lượng xử lý (Throughput): Đạt tối thiểu **3.000 events/giây** (kỳ vọng kiểm thử tải tới **8.000 events/giây**).
  * Độ trễ xử lý luồng (End-to-End Latency): Từ khi sự kiện phát sinh đến khi cập nhật cảnh báo trên Dashboard **< 3 giây**.
  * Tỷ lệ tối ưu lưu trữ HDFS: Giảm dung lượng vật lý tối thiểu **50%** nhờ định dạng nén Parquet so với CSV ban đầu.
* **Chất lượng mô hình & Nghiệp vụ (Analytics Metrics):**
  * Độ chính xác phát hiện quá tải: Tỷ lệ cảnh báo chính xác trùng khớp với các mốc giờ cao điểm lịch sử của New York đạt **F1-Score > 85%**.
  * Tính hợp lý của cụm K-Means: Đạt hệ số phân tách cụm **Silhouette Score > 0.6**.