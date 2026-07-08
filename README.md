# AQI Monitoring TP.HCM voi OpenAQ va Spark MLlib

Project mau cho bai toan theo doi va du bao chat luong khong khi do thi tai Thanh pho Ho Chi Minh.

## Kien truc

1. **Thu nhan du lieu**
   - `scripts/ingest_openaq.py` goi OpenAQ API v3 bang header `X-API-Key`.
   - Loc vi tri trong bbox TP.HCM, uu tien PM2.5 va PM10.
   - Khi chua co API key, chay `scripts/generate_sample_data.py` de tao du lieu mau nhu mang cam bien do thi.

2. **Luu tru column-oriented**
   - Du lieu duoc luu Parquet tai `data/parquet/measurements`.
   - Ingestion mac dinh append + dedup theo `sensor_id + parameter + datetime_utc`.
   - Dataset duoc partition theo `parameter/date`.
   - Dat `HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc` de route Parquet measurement qua Spark/Hadoop-compatible write path.

3. **Xu ly va phan tich**
   - `scripts/train_forecast_spark.py` dung PySpark MLlib voi 2 model: `RandomForestRegressor` va `GBTRegressor` (Gradient Boosted Trees).
   - Tao timeline theo gio day du, giu gap thanh null de lag 1h/3h/24h dung nghia.
   - Tao stacked target H+1 den H+24 voi `forecast_origin_ts`, `target_ts`, `horizon_hour`.
   - Chia train/validation/test theo thoi gian bang `target_ts` de tranh label leakage.
   - Ghi metrics that tai `data/predictions/metrics.json`: MAE, RMSE, R2 theo model/pollutant/horizon/split.

4. **API va truc quan hoa**
   - `app/main.py` phuc vu FastAPI.
   - API hien co: `/api/current`, `/api/forecast`, `/api/hotspots`, `/api/metrics`, `/api/health`, `/api/models`.
   - Frontend Leaflet co Current/Forecast mode, horizon H+1..H+24, Random Forest/GBT selector, hotspots, freshness va metrics neu artifact ton tai.

## Cai dat

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Sua `.env`:

```env
OPENAQ_API_KEY=your-key
```

OpenAQ API v3 can API key. Dang ky key tai OpenAQ Explorer.

## Chay nhanh voi du lieu mau

Dung luong du lieu mau du de demo heatmap va train Spark:

```powershell
python scripts/generate_sample_data.py --sensors 1200 --days 14 --overwrite
spark-submit scripts/train_forecast_spark.py
uvicorn app.main:app --reload
```

Mo: <http://127.0.0.1:8000>

Dashboard co dropdown chon `Random Forest` hoac `GBTRegressor`.

## Chay voi du lieu OpenAQ that

```powershell
python scripts/ingest_openaq.py --days 14 --limit-locations 80
spark-submit scripts/train_forecast_spark.py
uvicorn app.main:app --reload
```

Co the dung window ro rang:

```powershell
python scripts/ingest_openaq.py --datetime-from 2026-07-01T00:00:00Z --datetime-to 2026-07-08T00:00:00Z
```

Luu y: OpenAQ la tap hop du lieu cong khai da duoc OpenAQ phat hien/tich hop, khong bao dam co tat ca tram/cam bien tai TP.HCM. Neu khu vuc co it tram, generator du lieu mau giup ban demo mo hinh "hang ngan cam bien" nhu yeu cau de tai.

## HDFS

Neu co Hadoop/HDFS:

```env
HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc
```

Sau do chay lai ingest va train. Measurement Parquet va prediction Parquet se dung HDFS base path. Real HDFS cluster chua duoc verify trong repo nay; chi claim sau khi ban chay tren cluster that.

## Kiem thu

```powershell
python -m pytest
```

Tren Windows, Spark local co the can chay pytest ngoai sandbox/voi quyen day du de mo loopback va Python worker on dinh.

## Cau truc thu muc

```text
app/                     FastAPI + dashboard heatmap
src/                     cau hinh, OpenAQ client, AQI, data IO
scripts/                 ingest, sample generator, Spark train/forecast
data/parquet/            data lake local dang Parquet
data/predictions/        output du bao 24h dang JSON/Parquet
models/                  Spark ML model
docs/                    architecture, implementation plan, agent handoff
tests/                   AQI, Spark feature, storage, API, integration tests
```

## Nguon ky thuat

- OpenAQ API v3 dung API key qua header `X-API-Key`.
- OpenAQ v3 ho tro geospatial query bang `bbox` va `coordinates/radius`.
- Endpoint measurements cua OpenAQ v3 truy van theo `sensor_id`.
- OpenAQ v1/v2 da retired ngay 2025-01-31, nen project nay chi dung v3.
