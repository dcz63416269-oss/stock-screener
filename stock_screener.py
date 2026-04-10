import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ====================== 全局配置 ======================
# 筛选范围：仅A股主板（排除创业板、科创板、北交所）
EXCHANGE_MAIN_BOARD = ["SH", "SZ"]
EXCLUDE_BOARDS = ["创业板", "科创板", "北交所", "ST", "*ST"]

# ====================== 工具函数 ======================
def filter_main_board(df):
    """
    筛选仅A股主板股票，排除创业板、科创板、ST等
    """
    # 排除ST/*ST
    df = df[~df["股票名称"].str.contains("ST", na=False)]
    # 排除创业板（300开头）、科创板（688开头）、北交所（8开头）
    df = df[~df["股票代码"].str.startswith(("300", "688", "8"))]
    return df

def get_trading_days(start_date, end_date):
    """获取指定区间的交易日列表"""
    return ak.tool_trade_date_hist_sina()[(ak.tool_trade_date_hist_sina()["trade_date"] >= start_date) & 
                                          (ak.tool_trade_date_hist_sina()["trade_date"] <= end_date)]["trade_date"].tolist()

# ====================== 选股条件模块 ======================
def screen_basic(df):
    """基础筛选：市值、流动性、主板"""
    # 1. 仅保留主板
    df = filter_main_board(df)
    # 2. 市值筛选：50亿-500亿（中盘股，兼顾流动性和成长空间）
    df = df[(df["总市值"] > 5e9) & (df["总市值"] < 5e10)]
    # 3. 日均成交额筛选：近20日成交额>5000万，保证流动性
    df = df[df["近20日成交额"] > 5e7]
    return df

def screen_technical(df):
    """技术面筛选：趋势、均线、量能"""
    # 1. 股价站上20日均线，且20日均线向上（中期趋势向上）
    df = df[(df["最新价"] > df["20日均线"]) & (df["20日均线"] > df["40日均线"])]
    # 2. 近5日涨幅5%-20%，有上涨动能但不过度追高
    df = df[(df["近5日涨跌幅"] > 0.05) & (df["近5日涨跌幅"] < 0.2)]
    # 3. 近1日成交量放大（>20日均量的1.2倍）
    df = df[df["当日成交量"] > 1.2 * df["20日均量"]]
    # 4. RSI(14) 30-70，避免超买超卖
    df = df[(df["RSI14"] > 30) & (df["RSI14"] < 70)]
    return df

def screen_fundamental(df):
    """基本面筛选：估值、盈利、财务健康"""
    # 1. PE(TTM) 10-50，估值合理
    df = df[(df["市盈率(TTM)"] > 10) & (df["市盈率(TTM)"] < 50)]
    # 2. 资产负债率<70%，财务健康
    df = df[df["资产负债率"] < 0.7]
    # 3. 毛利率>20%，盈利能力强
    df = df[df["毛利率"] > 0.2]
    return df

# ====================== 【优化后】业绩预增选股模块 ======================
def get_performance_forecast(df):
    """
    业绩预增选股条件：净利润同比增长率>0
    优化点：
    1. 增加列名校验，彻底解决KeyError
    2. 兼容多数据源列名别名，自动匹配
    3. 异常兜底，列不存在不中断程序
    4. 增加日志输出，方便GitHub Actions调试
    """
    # 1. 打印当前列名，用于调试（上线可注释）
    print(f"[get_performance_forecast] 输入DataFrame列名：{df.columns.tolist()}")
    
    # 2. 定义目标列名 + 兼容常见别名，自动匹配数据源
    target_col = "净利润同比增长率"
    alias_list = [
        "净利润同比增长率", 
        "净利润同比", 
        "netprofit_yoy", 
        "净利润同比增长(%)",
        "净利润增速",
        "扣非净利润同比增长率"
    ]
    found_col = None
    for col in alias_list:
        if col in df.columns:
            found_col = col
            break
    
    # 3. 列不存在的兜底处理
    if not found_col:
        print(f"⚠️ 警告：未找到净利润增速相关列（尝试了{alias_list}），跳过业绩预增筛选")
        df[target_col] = pd.NA
        return df
    
    # 4. 统一列名 + 转数值类型，处理异常值
    df = df.rename(columns={found_col: target_col})
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    
    # 5. 过滤业绩预增（>0）且非空的股票，可调整阈值（比如>30%）
    df = df[df[target_col].notna() & (df[target_col] > 0)]
    print(f"✅ [get_performance_forecast] 业绩预增筛选后剩余股票数：{len(df)}")
    return df

# ====================== 数据获取模块 ======================
def fetch_stock_data():
    """获取全量A股股票数据，包含技术面、基本面、业绩数据"""
    print("开始获取A股全量股票数据...")
    
    # 1. 获取A股股票列表
    stock_list = ak.stock_zh_a_spot()
    stock_list = filter_main_board(stock_list)
    print(f"主板股票数量：{len(stock_list)}")
    
    # 2. 获取技术面数据（均线、量能、RSI等）
    tech_data = ak.stock_zh_a_indicator()
    # 3. 获取基本面数据（估值、财务等）
    fund_data = ak.stock_zh_a_fundamental()
    # 4. 获取业绩预告/财报数据（用于业绩预增筛选）
    perf_data = ak.stock_profit_forecast()
    
    # 5. 多表合并，统一索引
    df = stock_list.merge(tech_data, on="股票代码", how="left")
    df = df.merge(fund_data, on="股票代码", how="left")
    df = df.merge(perf_data, on="股票代码", how="left")
    
    # 6. 补充计算指标
    df["近20日成交额"] = df["近20日成交额"].fillna(0)
    df["20日均量"] = df["近20日成交量"] / 20
    df["当日成交量"] = df["成交量"]
    df["近5日涨跌幅"] = df["近5日涨跌幅"].fillna(0) / 100
    df["RSI14"] = df["RSI14"].fillna(50)
    df["资产负债率"] = df["资产负债率"].fillna(1)
    df["毛利率"] = df["毛利率"].fillna(0) / 100
    
    print(f"数据获取完成，合并后总条数：{len(df)}")
    return df

# ====================== 主筛选流程 ======================
def main_screen():
    """主筛选函数，按顺序执行所有选股条件"""
    # 1. 获取数据
    df = fetch_stock_data()
    
    # 2. 按顺序执行筛选条件
    print("\n开始执行选股条件...")
    df = screen_basic(df)
    print(f"基础筛选后剩余：{len(df)}")
    
    df = screen_technical(df)
    print(f"技术面筛选后剩余：{len(df)}")
    
    df = screen_fundamental(df)
    print(f"基本面筛选后剩余：{len(df)}")
    
    df = get_performance_forecast(df)
    print(f"业绩预增筛选后剩余：{len(df)}")
    
    # 3. 结果排序：按净利润同比增长率 + 近5日涨幅综合排序
    df = df.sort_values(by=["净利润同比增长率", "近5日涨跌幅"], ascending=False)
    
    # 4. 输出结果
    today = datetime.now().strftime("%Y-%m-%d")
    output_cols = [
        "股票代码", "股票名称", "最新价", "总市值", 
        "净利润同比增长率", "近5日涨跌幅", "市盈率(TTM)", 
        "20日均线", "RSI14", "毛利率"
    ]
    result = df[output_cols].head(10)  # 取前10只最优标的
    
    # 5. 保存结果
    result.to_csv(f"stock_screen_result_{today}.csv", index=False, encoding="utf-8-sig")
    print(f"\n✅ 选股完成！结果已保存为 stock_screen_result_{today}.csv")
    print("\n今日推荐标的：")
    print(result.to_string(index=False))
    
    return result

# ====================== 程序入口 ======================
if __name__ == "__main__":
    main_screen()
