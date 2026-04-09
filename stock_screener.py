#!/usr/bin/env python3
#### -*- coding: utf-8 -*-
"""
🦞 小龙虾竞价选股策略 - GitHub Actions 版
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import logging
import os

#### ========== 配置区 ==========
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")

PARAMS = {
    'min_open_gap': 3.0,
    'max_open_gap': 6.0,
    'min_volume_ratio': 2.0,
    'min_gain': 5.0,
    'min_price_ma20': True,
    'expma_bullish': True,
    'exclude_st': True,
    'main_board_only': True,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_main_board_code(code):
    code = str(code)
    return code.startswith('60') or code.startswith('000') or code.startswith('001') or code.startswith('002') or code.startswith('003')

def calculate_expma(series, span):
    return series.ewm(span=span, adjust=False).mean()

def get_realtime_data():
    logger.info("📊 获取全市场实时行情...")
    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return None
        df = df.rename(columns={'代码': 'code', '名称': 'name', '最新价': 'price', '涨跌幅': 'change_pct', '今开': 'open', '昨收': 'prev_close', '量比': 'volume_ratio'})
        logger.info(f"✅ 获取 {len(df)} 只股票行情")
        return df
    except Exception as e:
        logger.error(f"❌ 获取实时行情异常：{e}")
        return None

def quick_filter(df):
    logger.info("🔍 第一遍快速过滤...")
    filtered = df.copy()
    if PARAMS['exclude_st']:
        filtered = filtered[~filtered['name'].astype(str).str.contains('ST|*ST', na=False)]
    if PARAMS['main_board_only']:
        filtered = filtered[filtered['code'].apply(is_main_board_code)]
    filtered['open_gap'] = (filtered['open'] - filtered['prev_close']) / filtered['prev_close'] * 100
    filtered = filtered[(filtered['open_gap'] >= PARAMS['min_open_gap']) & (filtered['open_gap'] <= PARAMS['max_open_gap'])]
    filtered = filtered[filtered['volume_ratio'] > PARAMS['min_volume_ratio']]
    filtered = filtered[filtered['change_pct'] > PARAMS['min_gain']]
    filtered = filtered[filtered['price'] >= filtered['open']]
    logger.info(f"✅ 快速过滤后：{len(filtered)} 只股票")
    return filtered

def calculate_technical_indicators(code, name):
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        if hist_df is None or len(hist_df) < 60:
            return None
        close_series = hist_df['收盘'].astype(float)
        ma20 = close_series.rolling(window=20).mean().iloc[-1]
        exp20 = calculate_expma(close_series, 20).iloc[-1]
        exp60 = calculate_expma(close_series, 60).iloc[-1]
        latest_price = close_series.iloc[-1]
        return {'code': code, 'name': name, 'price': latest_price, 'ma20': ma20, 'exp20': exp20, 'exp60': exp60, 'price_above_ma20': latest_price > ma20, 'expma_bullish': exp20 > exp60}
    except Exception as e:
        return None

def apply_technical_filter(candidate_df):
    logger.info("📈 第二遍技术指标过滤...")
    qualified_stocks = []
    for idx, row in candidate_df.iterrows():
        tech_data = calculate_technical_indicators(row['code'], row['name'])
        if tech_data is None:
            continue
        if PARAMS['min_price_ma20'] and not tech_data['price_above_ma20']:
            continue
        if PARAMS['expma_bullish'] and not tech_data['expma_bullish']:
            continue
