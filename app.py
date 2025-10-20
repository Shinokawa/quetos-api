# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request
import akshare as ak
import yfinance as yf
import logging
import pandas as pd
import numpy as np
import schedule
import time
import threading
from datetime import datetime
import pytz
import json

# --- 初始化 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)
app.json.ensure_ascii = False

# --- 全局变量与锁 ---
quote_cache = {}
code_name_map = {}
cache_lock = threading.Lock()
CODE_MAP_FILE = 'code_name_map.json'

# --- 从本地JSON文件加载名称映射 ---
def load_code_name_map_from_file():
    global code_name_map
    try:
        app.logger.info(f"正在从文件 '{CODE_MAP_FILE}' 加载代码-名称映射表...")
        with open(CODE_MAP_FILE, 'r', encoding='utf-8') as f:
            temp_map = json.load(f)
        with cache_lock:
            code_name_map = temp_map
        app.logger.info(f"代码-名称映射表加载完成，总计 {len(code_name_map)} 条记录。")
    except FileNotFoundError:
        app.logger.error(f"严重错误：映射文件 '{CODE_MAP_FILE}' 未找到！")
    except Exception as e:
        app.logger.error(f"加载代码-名称映射表时发生严重错误: {e}")

# (其他辅助函数保持不变)
def is_trade_time():
    tz = pytz.timezone('Asia/Shanghai'); now = datetime.now(tz)
    if 1 <= now.isoweekday() <= 5:
        now_time = now.time()
        if (datetime.strptime("09:25", "%H:%M").time() <= now_time <= datetime.strptime("15:01", "%H:%M").time()) and \
           not (datetime.strptime("11:31", "%H:%M").time() < now_time < datetime.strptime("12:55", "%H:%M").time()):
            return True
    return False

def convert_to_yahoo_symbol(symbol):
    code_only = ''.join(filter(str.isdigit, symbol))
    if not code_only: return None
    if symbol.lower().startswith('sh') or code_only.startswith(('6', '5')): return f"{code_only}.SS"
    if symbol.lower().startswith('sz') or code_only.startswith(('0', '3', '1')): return f"{code_only}.SZ"
    return None

def map_yahoo_to_app_format(info, original_symbol):
    if not info or 'regularMarketPrice' not in info or info['regularMarketPrice'] is None:
         raise ValueError(f"雅虎财经未返回 {original_symbol} 的有效数据")
    def get_val(key, default=None): return info.get(key, default)
    change_pct = get_val('regularMarketChangePercent'); change_pct_formatted = f"{change_pct * 100:.2f}%" if change_pct is not None else None
    price, volume = get_val('regularMarketPrice'), get_val('regularMarketVolume'); turnover = price * volume if price is not None and volume is not None else None
    return {"名称": get_val('shortName'), "代码": original_symbol, "最新价": price, "开盘价": get_val('regularMarketOpen'), "最高价": get_val('regularMarketDayHigh'), "最低价": get_val('regularMarketDayLow'), "昨收": get_val('previousClose'), "涨跌额": get_val('regularMarketChange'), "涨跌幅": change_pct_formatted, "成交量": volume, "成交额": turnover, "买一": get_val('bid'), "卖一": get_val('ask'), "买一量": get_val('bidSize'), "卖一量": get_val('askSize')}

def clear_quote_cache():
    global quote_cache
    with cache_lock:
        if quote_cache: app.logger.info(f"清空旧的行情缓存，共 {len(quote_cache)} 条数据。"); quote_cache = {}

def run_schedule():
    app.logger.info("后台调度线程已启动。"); schedule.every().day.at("09:25", "Asia/Shanghai").do(clear_quote_cache)
    while True: schedule.run_pending(); time.sleep(1)

# --- 实时行情接口 ---
@app.route('/api/quotes', methods=['GET'])
def get_quotes():
    symbols_str = request.args.get('symbols')
    if not symbols_str: return jsonify({"error": "请提供'symbols'查询参数"}), 400
    symbol_list = [s.strip() for s in symbols_str.split(',')]
    results = {}; trade_time_now = is_trade_time()
    for symbol in symbol_list:
        try:
            if not trade_time_now and symbol in quote_cache:
                app.logger.info(f"[{symbol}] 命中收盘价缓存。"); results[symbol] = {"status": "success", "data": quote_cache[symbol]}; continue
            
            yahoo_symbol = convert_to_yahoo_symbol(symbol)
            if not yahoo_symbol: raise ValueError("无法识别的股票/ETF代码")
            
            app.logger.info(f"[{symbol} -> {yahoo_symbol}] 实时请求雅虎财经...")
            ticker = yf.Ticker(yahoo_symbol)
            info_data = ticker.info
            formatted_data = map_yahoo_to_app_format(info_data, symbol)
            
            # --- 【核心修复】构建正确的查询键(key)来获取中文名 ---
            code_only = ''.join(filter(str.isdigit, symbol))
            prefix = 'sh' if code_only.startswith(('6', '5')) else 'sz'
            full_symbol_for_lookup = f"{prefix}{code_only}"
            
            # 使用构建好的 key 进行查询
            formatted_data['名称'] = code_name_map.get(full_symbol_for_lookup, formatted_data['名称'])

            results[symbol] = {"status": "success", "data": formatted_data}
            if not trade_time_now:
                with cache_lock: quote_cache[symbol] = formatted_data
        except Exception as e:
            app.logger.error(f"处理代码 {symbol} 时发生错误: {e}"); results[symbol] = {"status": "error", "message": str(e)}
    return jsonify(results)

# --- 历史行情接口 (保持不变) ---
@app.route('/api/history', methods=['GET'])
def get_history():
    symbol = request.args.get('symbol'); start_date, end_date = request.args.get('start_date', '20200101'), request.args.get('end_date', datetime.now().strftime('%Y%m%d'))
    if not symbol: return jsonify({"status": "error", "message": "必须提供 'symbol' 参数。"}), 400
    try:
        code_only = ''.join(filter(str.isdigit, symbol)); history_df = None
        if code_only.startswith(('51', '56', '58', '15')): history_df = ak.fund_etf_hist_em(symbol=code_only, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        else: history_df = ak.stock_zh_a_hist(symbol=code_only, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        if history_df is None or history_df.empty: raise ValueError(f"Akshare 未返回有效数据。")
        history_df.replace({np.nan: None}, inplace=True); history_df['日期'] = history_df['日期'].astype(str)
        return jsonify({"status": "success", "symbol": symbol, "data": history_df.to_dict(orient='records')})
    except Exception as e:
        app.logger.error(f"获取历史行情 {symbol} 时发生错误: {e}"); return jsonify({"status": "error", "message": str(e)}), 500

# --- 主程序入口 ---
if __name__ == '__main__':
    app.logger.info("服务器以开发模式启动，开始初始化..."); load_code_name_map_from_file()
    scheduler_thread = threading.Thread(target=run_schedule); scheduler_thread.daemon = True; scheduler_thread.start()
    app.run(host='0.0.0.0', port=5000, debug=False)
elif __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error'); app.logger.handlers = gunicorn_logger.handlers; app.logger.setLevel(gunicorn_logger.level)
    app.logger.info("服务器由Gunicorn启动，开始初始化..."); load_code_name_map_from_file()
    scheduler_thread = threading.Thread(target=run_schedule); scheduler_thread.daemon = True; scheduler_thread.start()
