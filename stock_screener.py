import akshare as ak
import pandas as pd
from datetime import datetime

# ==========================
# 选股：仅主板 + 排除ST
# ==========================
def filter_main_board(df):
    # 必须存在的列
    if not all(col in df.columns for col in ["code", "name"]):
        return df

    # 过滤ST
    df = df[~df["name"].str.contains("ST|退|PT", na=False)]

    # 过滤创业板300/科创板688/北交所8
    df = df[
        ~df["code"].str.startswith(("300", "688", "8"))
    ]
    return df

# ==========================
# 获取业绩预告
# ==========================
def get_performance_forecast():
    try:
        df = ak.stock_profit_forecast()
        # 重命名适配
        df = df.rename(columns={
            "代码": "code",
            "净利润同比增长(%)": "profit_yoy"
        })
        df["profit_yoy"] = pd.to_numeric(df["profit_yoy"], errors="coerce")
        # 业绩预增 > 0
        df = df[df["profit_yoy"] > 0]
        return df[["code", "profit_yoy"]]
    except:
        return pd.DataFrame(columns=["code", "profit_yoy"])

# ==========================
# 主选股逻辑
# ==========================
def main_screen():
    print("正在获取 A 股实时数据...")

    # 1. 获取实时行情（唯一稳定接口）
    df = ak.stock_zh_a_spot()
    df = df.rename(columns={"symbol": "code"})

    # 2. 过滤主板
    df = filter_main_board(df)
    print(f"主板过滤后剩余: {len(df)}")

    # 3. 合并业绩预增
    perf = get_performance_forecast()
    df = df.merge(perf, on="code", how="left")
    df = df[df["profit_yoy"].notna()]
    print(f"业绩预增过滤后剩余: {len(df)}")

    # 4. 简单市值过滤（防止过小市值）
    if "total_shares" in df.columns and "close" in df.columns:
        df["market_cap"] = pd.to_numeric(df["total_shares"], errors="coerce") * pd.to_numeric(df["close"], errors="coerce")
        df = df[(df["market_cap"] >= 30 * 1e8) & (df["market_cap"] <= 500 * 1e8)]

    # 5. 排序
    df = df.sort_values("profit_yoy", ascending=False)

    # 6. 输出
    today = datetime.now().strftime("%Y-%m-%d")
    out = df[["code", "name", "close", "profit_yoy"]].head(15)
    out.to_csv(f"stock_result_{today}.csv", index=False, encoding="utf-8-sig")

    print("\n选股结果：")
    print(out)
    print(f"\n文件已保存: stock_result_{today}.csv")
    return out

if __name__ == "__main__":
    main_screen()
