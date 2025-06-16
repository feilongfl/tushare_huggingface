import tushare as ts
import akshare as ak
import pandas as pd
import time
from pathlib import Path
from loguru import logger
from tqdm import tqdm
import fire
from datetime import datetime


def get_trade_dates(start: str, end: str) -> list[str]:
    """使用 akshare 获取 A 股交易日（免费且稳定）"""
    df = ak.tool_trade_date_hist_sina()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    start_date = datetime.strptime(start, "%Y%m%d").date()
    end_date = datetime.strptime(end, "%Y%m%d").date()

    df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
    return [d.strftime("%Y%m%d") for d in df["trade_date"].sort_values(ascending=False)]

def fetch_all_daily(token: str, data_dir: str = "stock_data/daily",
                    start: str = "19800101", end: str = "20250616", retry: int = 3):
    ts.set_token(token)
    pro = ts.pro_api()

    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    dates = get_trade_dates(str(start), str(end))
    logger.info(f"Got {len(dates)} trading dates from {start} to {end}")

    def get_daily_by_date(date: str) -> pd.DataFrame:
        for attempt in range(retry):
            try:
                df = pro.daily(trade_date=date)
                if not df.empty:
                    return df
                return pd.DataFrame()
            except Exception as e:
                logger.warning(f"{date} attempt {attempt+1}/{retry} failed: {e}")
                time.sleep(1)
        logger.error(f"{date} failed after {retry} attempts")
        return pd.DataFrame()

    with tqdm(total=len(dates), desc="Fetching daily data") as pbar:
        for date in dates:
            out_file = data_path / f"{date}.parquet"
            if out_file.exists():
                pbar.update(1)
                continue
            df = get_daily_by_date(date)
            if not df.empty:
                df.to_parquet(out_file, index=False)
                logger.success(f"[{date}] Saved {len(df)} rows.")
            else:
                logger.info(f"[{date}] No data.")
            pbar.update(1)


if __name__ == "__main__":
    fire.Fire(fetch_all_daily)
