import akshare as ak
import pandas as pd
import numpy as np
from tqdm import tqdm
import requests

# ---------------- 配置 ----------------
START_DATE = "2020-01-01"
END_DATE   = "2020-02-01"             # None 表示到最新
HOLD_DAYS  = [1, 5, 10, 20]
TURNOVER_MAX = 5.0            # 换手率上限（%）
DAILY_RISE_MIN = 1.0          # 每日涨幅下限（%）
MIN_LISTED_DAYS = 60          # 次新过滤
EXCLUDE_ST = True
ak.session = requests.Session()
ak.session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0 Safari/537.36"
})

# --------------- 工具函数 ---------------
def get_a_stock_list():
    df = ak.stock_info_a_code_name()  # 沪深 A 股
    # 过滤 ST、退市、北交所（B 股与北交所可按需过滤）
    if EXCLUDE_ST:
        df = df[~df["name"].str.contains("ST")]
    # 去掉北交所：代码以 8 开头（新规下北交所 83/87 开头），也可按交易所字段过滤
    df = df[~df["code"].str.startswith(("8","4"))]
    return df[["code","name"]].reset_index(drop=True)

def fetch_stock_hist(code, start_date="20100101", end_date=None, adjust="qfq"):
    """
    拉取 A 股日线行情，带自动切换数据源
    code: 股票代码，如 "600000"
    start_date: 开始日期，YYYYMMDD
    end_date: 结束日期，YYYYMMDD or None
    adjust: "qfq" 前复权, "hfq" 后复权, "" 不复权
    """
    sources = [
        ("eastmoney", ak.stock_zh_a_hist),        # 东方财富
        # ("netease", ak.stock_zh_a_hist_163),      # 网易
        # ("sina", ak.stock_zh_a_hist_sina),        # 新浪
    ]

    last_err = None
    for name, func in sources:
        try:
            if name == "eastmoney":
                df = func(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust=adjust)
            elif name == "netease":
                df = func(symbol=code, start_date=start_date, end_date=end_date, adjust=adjust)
            elif name == "sina":
                df = func(symbol=code)
                # sina 的接口不支持指定日期，需自行过滤
                df = df[(df["date"] >= pd.to_datetime(start_date)) &
                        ((end_date is None) | (df["date"] <= pd.to_datetime(end_date)))]
            else:
                continue

            if df is not None and not df.empty:
                print(f"[INFO] 成功获取数据源: {name}, 股票: {code}")
                return df
        except Exception as e:
            print(f"[WARN] 数据源 {name} 失败: {e}")
            last_err = e
            continue

    print(f"[ERROR] 所有数据源都失败: {code}, error={last_err}")
    return pd.DataFrame()

def fetch_hist_k(code):
    # 复权价：前复权；包含换手率
    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=START_DATE.replace("-",""),
                            end_date=None if END_DATE is None else END_DATE.replace("-",""),
                            adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    # 统一列名
    df = df.rename(columns={
        "日期":"date","开盘":"open","收盘":"close","最高":"high","最低":"low","涨跌幅":"pct_chg",
        "换手率":"turnover","成交量":"vol","成交额":"amount"
    })
    # 数据清洗
    df["date"] = pd.to_datetime(df["date"])
    # ak 的“涨跌幅”为百分比（带 % 符号的字符串或已转为数值），统一成 float
    if df["pct_chg"].dtype == "O":
        df["pct_chg"] = df["pct_chg"].str.replace("%","").astype(float)
    else:
        df["pct_chg"] = df["pct_chg"].astype(float)
    if df["turnover"].dtype == "O":
        df["turnover"] = df["turnover"].str.replace("%","").replace("", np.nan).astype(float)
    else:
        df["turnover"] = df["turnover"].astype(float)
    df = df.sort_values("date").reset_index(drop=True)
    return df

def mark_signal(df):
    """ 标记严格条件：连续3天，日涨幅>1%，换手<5% """
    if df.empty or len(df) < MIN_LISTED_DAYS + 3:
        df["signal"] = False
        return df
    # 上市满 MIN_LISTED_DAYS
    df["listed_days"] = np.arange(1, len(df)+1)
    cond_len = df["listed_days"] >= MIN_LISTED_DAYS

    r = df["pct_chg"] > DAILY_RISE_MIN
    t = df["turnover"] < TURNOVER_MAX

    # 连续 3 天条件（逐日均满足）
    c3 = r & t
    sig = c3 & c3.shift(1) & c3.shift(2)
    sig = sig.fillna(False)

    df["signal"] = sig & cond_len
    return df

def compute_forward_stats(df, hold_days):
    """
    对标记为 signal 的日子 t，计算 t+N 收益：
    forward_ret_N = close[t+N]/close[t] - 1
    """
    if df.empty or not df["signal"].any():
        return {N: {"n":0,"win_rate":np.nan,"mean":np.nan,"median":np.nan} for N in hold_days}

    out = {}
    closes = df["close"].values
    sig_idx = np.where(df["signal"].values)[0]

    for N in hold_days:
        valid = sig_idx[sig_idx + N < len(df)]
        if len(valid) == 0:
            out[N] = {"n":0,"win_rate":np.nan,"mean":np.nan,"median":np.nan}
            continue
        rets = closes[valid + N] / closes[valid] - 1.0
        out[N] = {
            "n": int(len(rets)),
            "win_rate": float((rets > 0).mean()),
            "mean": float(np.mean(rets)),
            "median": float(np.median(rets)),
        }
    return out

# --------------- 主流程 ---------------
all_stats = {N: [] for N in HOLD_DAYS}
signals_per_stock = []

stock_list = get_a_stock_list()
for _, row in tqdm(stock_list.iterrows(), total=len(stock_list)):
    code, name = row["code"], row["name"]
    # df = fetch_hist_k(code)
    df = fetch_stock_hist(code)
    if df.empty:
        continue
    df = mark_signal(df)
    # 记录每只股票的信号数
    sig_count = int(df["signal"].sum())
    if sig_count > 0:
        signals_per_stock.append({"code":code,"name":name,"signals":sig_count})
    # 聚合每只股票的 forward 统计
    stats = compute_forward_stats(df, HOLD_DAYS)
    for N in HOLD_DAYS:
        if stats[N]["n"] > 0:
            all_stats[N].append(stats[N])

# 汇总全市场
summary = []
for N in HOLD_DAYS:
    if len(all_stats[N]) == 0:
        summary.append({"horizon":N,"signals":0,"win_rate":None,"mean":None,"median":None})
        continue
    dfN = pd.DataFrame(all_stats[N])
    total_signals = int(dfN["n"].sum())
    # 以“每个样本”为权重做加权平均
    win_rate = float((dfN["win_rate"] * dfN["n"]).sum() / total_signals)
    mean_ret = float((dfN["mean"] * dfN["n"]).sum() / total_signals)
    median_ret = float(dfN["median"].median())  # 中位数简单取中位
    summary.append({
        "horizon": N,
        "signals": total_signals,
        "win_rate": win_rate,
        "mean": mean_ret,
        "median": median_ret
    })

summary_df = pd.DataFrame(summary)
signals_df = pd.DataFrame(signals_per_stock).sort_values("signals", ascending=False)

print("=== 条件：连续3天 每日涨幅>1% & 每日换手<5% ===")
print(summary_df.to_string(index=False))
print("\n每只股票的信号出现次数（Top 20）：")
print(signals_df.head(20).to_string(index=False))