import os
import sys
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

#  LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

#  KONFIGURASI (dari Environment Variables)
DATABASE_URL = os.environ.get("DATABASE_URL")   # NeonDB connection string
SYMBOL       = "ETHUSDT"
INTERVAL     = "15m"
FETCH_LIMIT  = 2   # Ambil 2 candle terakhir: 1 confirmed + 1 safety
BINANCE_URL  = "https://api.binance.com/api/v3/klines"

#  BINANCE FETCH
def fetch_latest_klines(symbol: str, interval: str, limit: int = 2) -> list:
    """Ambil candle terbaru dari Binance Public API."""
    params = {
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit,
    }
    resp = requests.get(BINANCE_URL, params=params, timeout=15)
    resp.raise_for_status()
    raw = resp.json()

    # Ambil hanya candle yang sudah CLOSED (bukan candle berjalan)
    # Candle terakhir (index -1) masih berjalan, ambil index -2
    closed_candles = raw[:-1]
    log.info(f"Fetched {len(closed_candles)} closed candle(s) dari Binance")
    return closed_candles


def parse_kline(raw: list, symbol: str) -> dict:
    """Parse satu raw kline Binance menjadi dict."""
    return {
        "symbol":       symbol,
        "open_time":    datetime.fromtimestamp(raw[0] / 1000, tz=timezone.utc),
        "open":         float(raw[1]),
        "high":         float(raw[2]),
        "low":          float(raw[3]),
        "close":        float(raw[4]),
        "volume":       float(raw[5]),
        "close_time":   datetime.fromtimestamp(raw[6] / 1000, tz=timezone.utc),
        "quote_volume": float(raw[7]),
        "trades":       int(raw[8]),
    }

#  NEONDB
def get_connection():
    """Buat koneksi ke NeonDB via DATABASE_URL."""
    if not DATABASE_URL:
        raise EnvironmentError("DATABASE_URL tidak ditemukan di environment variables!")
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    return conn


def create_table_if_not_exists(conn):
    """Buat tabel jika belum ada (idempotent)."""
    sql = """
    CREATE TABLE IF NOT EXISTS eth_usdt_15m (
        id           BIGSERIAL PRIMARY KEY,
        symbol       VARCHAR(20)              NOT NULL,
        open_time    TIMESTAMPTZ              NOT NULL,
        open         NUMERIC(20, 8)           NOT NULL,
        high         NUMERIC(20, 8)           NOT NULL,
        low          NUMERIC(20, 8)           NOT NULL,
        close        NUMERIC(20, 8)           NOT NULL,
        volume       NUMERIC(30, 8)           NOT NULL,
        close_time   TIMESTAMPTZ              NOT NULL,
        quote_volume NUMERIC(30, 8)           NOT NULL,
        trades       INTEGER                  NOT NULL,
        inserted_at  TIMESTAMPTZ              NOT NULL DEFAULT NOW(),

        CONSTRAINT eth_usdt_15m_unique_candle UNIQUE (symbol, open_time)
    );

    -- Index untuk query time-series
    CREATE INDEX IF NOT EXISTS idx_eth_usdt_15m_open_time
        ON eth_usdt_15m (open_time DESC);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("Tabel eth_usdt_15m siap.")


def upsert_klines(conn, records: list[dict]) -> int:
    """
    Insert candles ke NeonDB.
    Pakai ON CONFLICT DO NOTHING agar aman jika candle sudah ada.
    Returns jumlah baris yang benar-benar diinsert.
    """
    if not records:
        return 0

    sql = """
    INSERT INTO eth_usdt_15m
        (symbol, open_time, open, high, low, close,
         volume, close_time, quote_volume, trades)
    VALUES %s
    ON CONFLICT (symbol, open_time) DO NOTHING
    """

    values = [
        (
            r["symbol"], r["open_time"], r["open"],  r["high"],
            r["low"],    r["close"],    r["volume"], r["close_time"],
            r["quote_volume"], r["trades"],
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        inserted = cur.rowcount

    conn.commit()
    return inserted
  
#  MAIN
def main():
    log.info("=" * 50)
    log.info(f"  ETH/USDT 15m Real-Time Fetcher")
    log.info(f"  Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("=" * 50)

    # 1. Fetch dari Binance
    try:
        raw_klines = fetch_latest_klines(SYMBOL, INTERVAL, FETCH_LIMIT)
    except requests.exceptions.RequestException as e:
        log.error(f"Gagal fetch dari Binance: {e}")
        sys.exit(1)

    if not raw_klines:
        log.warning("Tidak ada candle closed yang diterima. Skip.")
        sys.exit(0)

    # 2. Parse
    records = [parse_kline(k, SYMBOL) for k in raw_klines]
    for r in records:
        log.info(
            f"  Candle: {r['open_time'].strftime('%Y-%m-%d %H:%M')} UTC | "
            f"O={r['open']} H={r['high']} L={r['low']} C={r['close']} "
            f"V={r['volume']:.2f}"
        )

    # 3. Simpan ke NeonDB
    try:
        conn = get_connection()
        create_table_if_not_exists(conn)
        inserted = upsert_klines(conn, records)
        conn.close()
    except Exception as e:
        log.error(f"Gagal simpan ke NeonDB: {e}")
        sys.exit(1)

    log.info(f"Selesai. {inserted} candle baru diinsert ke NeonDB.")


if __name__ == "__main__":
    main()
