# AQI Monitoring TP.HCM voi OpenAQ va Spark MLlib

Project mau cho bai toan theo doi va du bao chat luong khong khi do thi tai Thanh pho Ho Chi Minh.

## Kien truc

1. **Thu nhan du lieu**
   - `scripts/ingest_openaq.py` goi OpenAQ API v3 bang header `X-API-Key`.
   - Loc vi tri trong bbox TP.HCM, uu tien PM2.5 va PM10.
   - Khi chua co API key, chay `scripts/generate_sample_data.py` de tao du lieu mau nhu mang cam bien do thi.

2. **Luu tru column-oriented**
   - Du lieu duoc luu Parquet tai `data/parquet/measurements`.
   - Dat `HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc` de ghi doc tren HDFS.
   - Parquet phu hop cho truy van cot, nen Spark chi doc cac cot can thiet khi train va ve heatmap.

3. **Xu ly va phan tich**
   - `scripts/train_forecast_spark.py` dung PySpark MLlib voi 2 model: `RandomForestRegressor` va `GBTRegressor` (Gradient Boosted Trees).
   - Tao dac trung theo gio, thu trong tuan, toa do luoi, do tre 1h/3h/24h.
   - Du bao PM2.5/PM10 va AQI cho 24 gio tiep theo, co the so sanh output theo tung model.

4. **Truc quan hoa realtime**
   - `app/main.py` phuc vu API FastAPI.
   - `app/static/index.html` hien thi ban do Leaflet + heatmap layer cho AQI hien tai va du bao 24h.

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
python scripts/generate_sample_data.py --sensors 1200 --days 14
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

Luu y: OpenAQ la tap hop du lieu cong khai da duoc OpenAQ phat hien/tich hop, khong bao dam co tat ca tram/cam bien tai TP.HCM. Neu khu vuc co it tram, generator du lieu mau giup ban demo mo hinh "hang ngan cam bien" nhu yeu cau de tai.

## HDFS

Neu co Hadoop/HDFS:

```env
HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc
```

Sau do chay lai ingest va train. Cac path Parquet/prediction se nam trong HDFS base path.

## Cau truc thu muc

```text
app/                     FastAPI + dashboard heatmap
src/                     cau hinh, OpenAQ client, AQI, data IO
scripts/                 ingest, sample generator, Spark train/forecast
data/parquet/            data lake local dang Parquet
data/predictions/        output du bao 24h dang JSON/Parquet
models/                  Spark ML model
```

## Nguon ky thuat

- OpenAQ API v3 dung API key qua header `X-API-Key`.
- OpenAQ v3 ho tro geospatial query bang `bbox` va `coordinates/radius`.
- Endpoint measurements cua OpenAQ v3 truy van theo `sensor_id`.
- OpenAQ v1/v2 da retired ngay 2025-01-31, nen project nay chi dung v3.
