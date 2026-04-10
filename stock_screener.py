import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import os

def main():
    # 从环境变量读取 token，避免硬编码被 GitHub 泄露
    TOKEN = os.getenv("TUSHARE_TOKEN")
    if not TOKEN:
        print("❌ 未设置 TUSHARE_TOKEN 环境变量")
        return

    ts.set_token(TOKEN)
    pro = ts.pro_api()

    # 选股参数
    MIN_GROWTH_RATE = 20  # 业绩预增最低幅度(%)

    def get_latest_trade_date():
        now = datetime.now()
        for i in range(7):
            d = now - timedelta(days=i)
            if d.weekday() < 5:
                return d.strftime("%Y%m%d")
        return now.strftime("%Y%m%d")

    trade_date = get_latest_trade_date()
    print("📅 交易日期:", trade_date)

    # 1. 主板股票列表
    try:
        stock_basic = pro.stock_basic(exchange='', list_status='L',
                                       fields='ts_code,symbol,name,market,list_date')
    except Exception as e:
        print("❌ 获取股票列表失败:", e)
        return

    # 只保留沪主板(60) + 深主板(000)，排除创业板/科创板/北交所
    main_board = stock_basic[
        stock_basic['ts_code'].str.match(r'^(60|000)')
    ].copy()
    main_board = main_board.rename(columns={'ts_code': 'code'})
    print(f"✅ 主板股票数量: {len(main_board)}")

    # 2. 业绩预增
    try:
        fore = pro.forecast(
            start_date=(datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=90)).strftime("%Y%m%d"),
            end_date=trade_date,
            fields='ts_code,ann_date,type,p_change_min,p_change_max'
        )
    except Exception as e:
        print("❌ 获取业绩预告失败:", e)
        return

    if fore.empty:
        print("⚠️ 无业绩预告数据")
        return

    fore = fore[fore['type'].isin(['预增', '扭亏'])].copy()
    fore = fore[fore['p_change_min'] >= MIN_GROWTH_RATE].copy()
    fore = fore.sort_values(['ts_code', 'ann_date'], ascending=[True, False]).drop_duplicates('ts_code')
    fore = fore.rename(columns={'ts_code': 'code'})

    # 3. 交集：主板 + 业绩预增
    df = pd.merge(main_board, fore, on='code', how='inner')
    if df.empty:
        print("⚠️ 无同时满足主板+业绩预增的股票")
        return

    # 4. 行情与市值
    try:
        daily = pro.daily(trade_date=trade_date, fields='ts_code,close,volume_ratio,pct_chg')
        basic = pro.daily_basic(trade_date=trade_date, fields='ts_code,circ_mv')
    except Exception as e:
        print("❌ 获取行情数据失败:", e)
        return

    daily = daily.rename(columns={'ts_code': 'code'})
    basic = basic.rename(columns={'ts_code': 'code'})
    basic['circ_mv_yi'] = basic['circ_mv'] / 100000000

    df = df.merge(daily, on='code', how='left')
    df = df.merge(basic, on='code', how='left')
    df = df.dropna(subset=['close', 'circ_mv_yi'])

    # 5. 输出结果
    df = df.sort_values('p_change_min', ascending=False).reset_index(drop=True)
    print("\n🎯 最终筛选结果")
    print(df[['code', 'name', 'pct_chg', 'volume_ratio', 'circ_mv_yi', 'p_change_min']].head(20))

    # 保存
    df.to_csv("stock_screen_result.csv", index=False, encoding="utf-8-sig")
    print("\n✅ 结果已保存为 stock_screen_result.csv")

if __name__ == "__main__":
    main()
