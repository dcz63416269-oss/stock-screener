import akshare as ak
import pandas as pd
import requests

# ====================== 策略参数 ======================
NET_PROFIT_GROWTH_MIN = 30   # 净利润同比增速 ≥30%
TURNOVER_MIN = 3
TURNOVER_MAX = 15
VOL_RATIO = 1.8
STOP_AFTER_DAYS = 10
# ======================================================

def get_main_board_stocks():
    """获取 A 股主板股票列表（60/000/001 开头）"""
    df = ak.stock_zh_a_spot_em()
    df = df[df["代码"].str.match(r"^(60|000|001)")]
    return df["代码"].tolist()

def get_performance_forecast():
    """获取业绩预增股票：净利润同比≥30%"""
    df = ak.stock_yjyg_em()
    df["净利润同比增长率"] = pd.to_numeric(df["净利润同比增长率"], errors="coerce")
    df = df[df["净利润同比增长率"] >= NET_PROFIT_GROWTH_MIN]
    return df["股票代码"].tolist()

def screening():
    print("开始选股...")

    # 1. 主板列表
    main_codes = get_main_board_stocks()

    # 2. 业绩预增列表
    yj_codes = get_performance_forecast()

    # 3. 交集：主板 + 业绩预增
    target_codes = list(set(main_codes) & set(yj_codes))
    if not target_codes:
        return "今日无符合条件股票"

    # 4. 取日K数据
    result = []
    for code in target_codes[:300]:
        try:
            df = ak.stock_zh_a_daily(symbol=code, adjust="qfq")
            if len(df) < 60:
                continue

            # 均线
            df["ma5"] = df["close"].rolling(5).mean()
            df["ma10"] = df["close"].rolling(10).mean()
            df["ma20"] = df["close"].rolling(20).mean()
            df["ma60"] = df["close"].rolling(60).mean()
            df["vol_20"] = df["volume"].rolling(20).mean()

            latest = df.iloc[-1]
            prev = df.iloc[-2]

            # 均线多头
            long_condition = (
                latest.close > latest.ma5 > latest.ma10 > latest.ma20 > latest.ma60
                and latest.ma5 > prev.ma5
            )

            # 成交量
            vol_condition = latest.volume >= latest.vol_20 * VOL_RATIO

            # 换手率
            turn_condition = TURNOVER_MIN <= latest.turnover <= TURNOVER_MAX

            if long_condition and vol_condition and turn_condition:
                result.append({
                    "code": code,
                    "name": ak.stock_individual_f10_em(symbol=code)["company_name"][0],
                    "close": round(latest.close, 2),
                    "turnover": round(latest.turnover, 2),
                })
        except Exception as e:
            continue

    if not result:
        return "今日无符合条件股票"

    msg = "【主板+业绩预增+均线多头选股结果】\n"
    for idx, r in enumerate(result[:10], 1):
        msg += f"{idx}. {r['code']} {r['name']} | 价:{r['close']} | 换手:{r['turnover']}%\n"

    return msg

def send_to_feishu(text):
    import os
    url = os.getenv("FEISHU_WEBHOOK_URL")
    if not url:
        print(text)
        return
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        requests.post(url, json=payload)
    except:
        pass

if __name__ == "__main__":
    res = screening()
    send_to_feishu(res)
