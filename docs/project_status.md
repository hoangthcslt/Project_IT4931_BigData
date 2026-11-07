# ============================================================================
# PROJECT STATUS & AI CODING AGENT HANDOFF GUIDE
# NYC Transit Real-Time Passenger Flow & Overcrowding Analytics System (IT4931)
# ============================================================================

> **Dành cho thành viên nhóm & AI Coding Agents:**  
> Tài liệu này tóm tắt chính xác hiện trạng mã nguồn, dữ liệu, kết quả thực nghiệm đã chạy và danh sách công việc tiếp theo (Next Steps). Khi bắt đầu phiên làm việc mới với AI Agent, chỉ cần yêu cầu Agent đọc `docs/overview.md` và `docs/project_status.md`.

---

## 1. TỔNG QUAN HIỆN TRẠNG (WHERE WE ARE NOW)

Dự án đã hoàn thành **Giai đoạn 1 (Đặc tả kiến trúc & Khảo sát dữ liệu)** và **Giai đoạn 2 (Xây dựng toàn bộ Pipeline mã nguồn 4 tầng Lambda Architecture + Kiểm thử thực nghiệm cục bộ)**.

### Trạng thái từng hạng mục:

| Hạng mục / Tầng kiến trúc | Tệp tin chính | Trạng thái | Kết quả kiểm chứng thực tế |
| :--- | :--- | :---: | :--- |
| **1. Hạ tầng & Cấu hình** | `docker-compose.yml`, `.env`, `requirements.txt` | **Hoàn thành (100%)** | Đã cấu hình đầy đủ 8 container (`Zookeeper`, `Kafka`, `HDFS NameNode/DataNode`, `Spark Master/Worker`, `Elasticsearch`, `Kibana`) + biến môi trường `.env`. |
| **2. Dữ liệu nguồn (`data/archive/`)** | `TurnstileData/` (27 files), `MTA_StationLocations.csv`, `NYC_TransitSubway/`, `NYC_TransitPerformance/` | **Hoàn thành (100%)** | Đã bỏ `data/` khỏi `.gitignore` và đẩy trực tiếp lên GitHub để mọi thành viên clone về chạy ngay không cần tải ngoài. |
| **3. Entity Resolution & Spatial Join** | `src/batch/entity_resolution.py` | **Hoàn thành (100%)** | Ánh xạ **459/459 (100.00%)** tổ hợp trạm MTA Subway hợp lệ sang `Station ID / Complex ID` và tọa độ chuẩn; tự động lọc bỏ 22 tổ hợp ngoại lai (`PTH`, `RIT`). |
| **4. Batch Layer: Làm sạch & Baseline** | `src/batch/clean_turnstile.py` | **Hoàn thành (100%)** | Đã chạy Full-Scale trên **toàn bộ 27 tệp tuần (5.254.533 dòng)**: lọc 314.371 dòng ngoại lai, 51.319 lỗi quay ngược bộ đếm ($\Delta < 0$) và 702 xung số ảo, thu được **4.883.723 khoảng hợp lệ** trên **437 ga** và xuất `data/processed/station_baseline.json` (73.416 cấu hình `Mean, Std`). |
| **5. Batch Layer: Spark MLlib K-Means** | `src/batch/kmeans_clustering.py` | **Hoàn thành (100%)** | Phân cụm toàn bộ **437 nhà ga** theo vector hành vi 24 giờ thành 3 cụm (*Residential Commuter Satellite*: 263, *CBD Financial & Office Hub*: 120, *All-Day Commercial & Tourist Hub*: 54). Đạt **Silhouette Score = `0.6777`** (vượt xa chỉ tiêu `> 0.6`). |
| **6. Batch Layer: Tương quan KPI Trễ tàu** | `src/batch/correlation_analysis.py` | **Hoàn thành (100%)** | Đã chạy Full-Scale 6 tháng đầu năm 2016: ghép nối **147 bản ghi tháng-tuyến** trên **21 tuyến tàu** với `Subway Wait Assessment` ($r = -0.2276$) và `OTP Terminal` ($r = -0.1110$). Xuất `data/processed/correlation_results.json`. |
| **7. Ingestion Layer: Event Simulator** | `src/ingestion/flow_interpolator.py`, `src/ingestion/kafka_producer.py` | **Hoàn thành (100%)** | Phân rã chu kỳ 4 giờ thành micro-events theo trọng số giờ cao điểm & phân phối Poisson. Đạt thông lượng kiểm thử **~4.978 events/giây** (`--mode interpolate` & `--mode replay`). |
| **8. Speed Layer: Streaming & Z-Score** | `src/streaming/stream_processor.py`, `src/streaming/anomaly_detector.py` | **Hoàn thành (100%)** | Xử lý 80.000 micro-events trên **7.775 cửa sổ trượt 15 phút (trượt 1 phút)** với độ trễ **`1.485 giây` (< 3 giây)**, phát hiện **691 cảnh báo `CRITICAL_OVERCROWDING_ALERT`** ($\text{Z-Score} > 3.0$). Xuất `data/processed/realtime_alerts.json`. |
| **9. Serving Layer & Live Dashboard** | `src/serving/es_indexer.py`, `src/serving/dashboard_server.py`, `src/serving/generate_report_figures.py` | **Hoàn thành (100%)** | Khởi tạo 4 chỉ mục Elasticsearch (`geo_point`), xuất cấu hình Kibana (`docs/kibana_dashboard.ndjson`), tạo **Interactive Live Web Dashboard (`docs/live_dashboard.html`)** và **4 biểu đồ PNG độ phân giải cao (1600x1000)** tại `docs/figures/`. |
| **10. Notebook & Unit Tests (`pytest`)** | `notebooks/exploratory_analysis.ipynb`, `tests/test_lambda_pipeline.py`, `run_pipeline.ps1` | **Hoàn thành (100%)** | Bộ 7 kiểm thử tự động `pytest` đạt **7/7 PASSED (100%)**, kiểm chứng toàn bộ tiêu chí nghiệm thu (Entity Resolution 100%, Nén > 50%, Silhouette = 0.6777 > 0.60, F1-Score > 85%, Latency < 3s). Kèm script chạy 1 chạm `run_pipeline.ps1`. |

---

## 2. SƠ ĐỒ CẤU TRÚC TỆP HIỆN TẠI (REPOSITORY MAP)

```text
Project_IT4931_BigData/
├── .env                                # Biến môi trường dùng chung cho toàn dự án
├── .gitignore                          # Chỉ bỏ qua venv, __pycache__, spark_checkpoint (GIỮ LẠI data/)
├── README.md                           # Hướng dẫn tổng quan và cách cài đặt, vận hành
├── docker-compose.yml                  # Cụm 8 dịch vụ Docker (Zookeeper, Kafka, HDFS, Spark, ES, Kibana)
├── run_pipeline.ps1                    # Script 1-chạm chạy toàn bộ Pipeline, Pytest & mở Live Dashboard
├── requirements.txt                    # Danh sách thư viện Python 3.11+
├── docs/
│   ├── overview.md                     # Đặc tả kỹ thuật chi tiết & Kiến trúc Lambda của dự án
│   ├── project_status.md               # [FILE NÀY] Tóm tắt hiện trạng & Handoff cho Agent/Team
│   ├── live_dashboard.html             # Interactive Live Web Dashboard (Leaflet Map + Chart.js + Live Sim)
│   ├── kibana_dashboard.ndjson         # Tệp cấu hình Saved Objects import 1-click vào Kibana
│   └── figures/                        # Bộ 4 biểu đồ PNG (1600x1000) từ tập dữ liệu Full-Scale 27 tuần
│       ├── fig1_kmeans_24h_profiles.png
│       ├── fig2_nyc_subway_geo_clusters.png
│       ├── fig3_wait_assessment_correlation.png
│       └── fig4_speed_layer_zscore_alerts.png
├── data/
│   ├── archive/                        # Dữ liệu gốc MTA (Turnstile 27 tuần, Locations, GTFS, Performance)
│   └── processed/                      # Dữ liệu đầu ra đã chạy kiểm chứng:
│       ├── es_index_mappings.json      # Schema 4 chỉ mục Elasticsearch (kèm geo_point)
│       ├── station_baseline.json       # 72.912 hồ sơ lịch sử (Mean, Std theo trạm/thứ/giờ)
│       ├── station_clusters.json       # Kết quả phân cụm K-Means 434 trạm (Silhouette = 0.6089)
│       ├── correlation_results.json    # Kết quả tương quan lưu lượng vs Subway Wait Assessment 2016
│       └── realtime_alerts.json        # Kết quả Sliding Window 15m, Top 5 trạm quá tải & 150 cảnh báo Z>3
├── src/
│   ├── ingestion/
│   │   ├── flow_interpolator.py        # Nội suy chu kỳ 4h thành micro-events theo giờ cao điểm + Poisson
│   │   └── kafka_producer.py           # Producer đẩy dữ liệu vào Kafka (chế độ interpolate / replay)
│   ├── batch/
│   │   ├── entity_resolution.py        # Ánh xạ 100% tên trạm Turnstile sang MTA_StationLocations.csv
│   │   ├── clean_turnstile.py          # Làm sạch bộ đếm (Lag window), ghi Parquet & tính Baseline
│   │   ├── kmeans_clustering.py        # Spark MLlib K-Means gom cụm hành vi nhà ga 24h
│   │   └── correlation_analysis.py     # Tương quan lưu lượng với MTA Wait Assessment & OTP (ISO-8859-1)
│   ├── streaming/
│   │   ├── anomaly_detector.py         # Thuật toán Z-Score & phân loại mức độ quá tải
│   │   └── stream_processor.py         # Spark Structured Streaming (15m window, 1m slide)
│   └── serving/
│       ├── es_indexer.py               # Quản lý chỉ mục geo_point & Bulk đồng bộ vào Elasticsearch
│       ├── dashboard_server.py         # Tạo Live HTML Dashboard, Kibana NDJSON & FastAPI Server (:8050)
│       └── generate_report_figures.py  # Xuất 4 biểu đồ PNG chất lượng cao vào docs/figures/
├── tests/
│   └── test_lambda_pipeline.py         # Bộ 7 Unit & Acceptance Tests kiểm chứng toàn bộ SLA nghiệm thu
└── notebooks/
    └── exploratory_analysis.ipynb      # Notebook EDA & trực quan hóa kết quả phục vụ báo cáo
```

---

## 3. CƠ CHẾ DUAL-ENGINE (CHẠY ĐƯỢC CẢ KHI CÓ HOẶC KHÔNG CÓ DOCKER/SPARK)

Một điểm thiết kế quan trọng của mã nguồn hiện tại là **mọi module trong `src/` đều hỗ trợ `--engine auto` (Dual-Engine)**:
* **Khi chạy trên cụm Docker / Spark (`--engine spark` hoặc `spark-submit`):** Sử dụng `SparkSession`, `Window.partitionBy`, `Spark MLlib KMeans`, `Spark Structured Streaming` kết nối trực tiếp tới Kafka (`localhost:9092`), HDFS (`hdfs://namenode:9000`) và Elasticsearch (`http://localhost:9200`).
* **Khi chạy trên máy cá nhân chưa bật Docker (`--engine local` hoặc `--engine auto`):** Tự động chuyển sang engine xử lý cục bộ bằng Python chuẩn để tính toán chính xác cùng công thức toán học và cập nhật các tệp kết quả trong `data/processed/`. Nhờ vậy, bất kỳ thành viên nào cũng có thể test code ngay lập tức chỉ với Python.

---

## 4. CẦN LÀM GÌ TIẾP THEO? (NEXT STEPS FOR DEVELOPERS / AGENTS)

Nếu bạn là thành viên tiếp nhận repo hoặc là AI Coding Agent được giao nhiệm vụ tiếp theo, dưới đây là các hạng mục có thể triển khai tiếp:

### Ưu tiên 1: Chạy thực nghiệm Full-Scale trên toàn bộ 27 tệp Turnstile (5,18 triệu dòng)
* Hiện tại các tệp trong `data/processed/` được sinh nhanh từ tập mẫu (`--max-files 4` và `--max-files 6`) để kiểm thử tức thời.
* Để chạy trên **toàn bộ 27 tệp tuần (5,18 triệu dòng)**, chỉ cần bỏ tham số `--max-files`:
  ```bash
  python src/batch/clean_turnstile.py --engine local
  python src/batch/kmeans_clustering.py --engine local
  python src/batch/correlation_analysis.py
  python src/streaming/stream_processor.py --engine local --max-events 100000
  ```

### Ưu tiên 2: Khởi chạy cụm Docker Compose & Đồng bộ lên Kibana Dashboard
1. Bật Docker Desktop (cấp tối thiểu 8GB RAM) và chạy:
   ```bash
   docker compose up -d
   ```
2. Khởi tạo các chỉ mục `geo_point` và đẩy toàn bộ dữ liệu từ `data/processed/*.json` vào Elasticsearch:
   ```bash
   python src/serving/es_indexer.py --init-index --sync-all
   ```
3. Mở Kibana tại `http://localhost:5601`, tạo Index Patterns (`mta-station-clusters*`, `mta-realtime-flow*`, `mta-congestion-alerts*`) và thiết kế **Kibana Maps Heatmap** + **Top 5 Emergency Stations Table**, sau đó xuất file cấu hình giao diện lưu vào `docs/kibana_dashboard.ndjson`.

### Ưu tiên 3: Viết Unit Tests & Tự động hóa kịch bản Demo
* Tạo thư mục `tests/` (dùng `pytest`) để kiểm thử tự động:
  * Kiểm thử `StationEntityResolver` đạt 100% trên 459 trạm MTA và lọc sạch trạm PATH/RIT.
  * Kiểm thử bộ lọc counter rollover ($\Delta < 0$) và spike ($\Delta > 7.200 \times \Delta t_{hours}$).
  * Kiểm thử công thức Z-Score ($\text{Z-Score} > 3.0 \implies \text{CRITICAL\_OVERCROWDING\_ALERT}$).
* Tạo script một chạm `run_pipeline.ps1` / `run_pipeline.sh` để chạy tuần tự từ Bước 1 đến Bước 4 khi thuyết trình bảo vệ đồ án.
