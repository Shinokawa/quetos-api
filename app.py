# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request
import akshare as ak
import logging
import pandas as pd
import schedule
import time
import threading
from datetime import datetime
import pytz

# --- 初始化 ---
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
# 优化: 让 jsonify 返回的 json 字符串能直接显示中文
app.json.ensure_ascii = False

# --- 全局变量与锁 ---
etf_cache_df = None
stock_cache = {} # 用于缓存股票收盘价数据
cache_lock = threading.Lock()

# --- 核心函数 ---
def is_trade_time():
    """判断当前是否为A股交易时间。"""
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    # 判断是否为周一至周五
    if 1 <= now.isoweekday() <= 5:
        # 判断是否在交易时间段内
        now_time = now.time()
        if (datetime.strptime("09:25", "%H:%M").time() <= now_time <= datetime.strptime("11:31", "%H:%M").time()) or \
           (datetime.strptime("12:55", "%H:%M").time() <= now_time <= datetime.strptime("15:01", "%H:%M").time()):
            return True
    return False

def update_etf_cache():
    """
    获取并更新全局的ETF行情缓存。
    【升级】: 当主数据源(东方财富)失败时，自动切换到备用数据源(同花顺)。
    """
    global etf_cache_df
    app.logger.info("计划任务触发：准备更新ETF缓存...")
    temp_df = None
    
    # --- 优先尝试主数据源：东方财富 ---
    try:
        app.logger.info("尝试从主数据源 [东方财富] 获取ETF行情...")
        temp_df = ak.fund_etf_spot_em()
        if temp_df is None or temp_df.empty:
            app.logger.warning("主数据源 [东方财富] 返回了空数据，将尝试备用数据源。")
            raise ValueError("Empty data from primary source")
        else:
            temp_df.set_index('代码', inplace=True)
            app.logger.info(f"主数据源 [东方财富] 获取成功！共 {len(temp_df)} 条数据。")

    except Exception as e:
        app.logger.error(f"从主数据源获取ETF行情失败: {e}。切换到备用数据源 [同花顺]...")
        
        # --- 切换到备用数据源：同花顺 ---
        try:
            ths_df = ak.fund_etf_spot_ths()
            if ths_df is not None and not ths_df.empty:
                app.logger.info(f"备用数据源 [同花顺] 获取成功！共 {len(ths_df)} 条数据。")
                # --- 数据标准化：重命名列以匹配主数据源的格式 ---
                standardized_df = pd.DataFrame()
                standardized_df['代码'] = ths_df['基金代码']
                standardized_df['名称'] = ths_df['基金名称']
                standardized_df['最新价'] = ths_df['当前-单位净值']
                standardized_df['涨跌幅'] = pd.to_numeric(ths_df['增长率'], errors='coerce')
                standardized_df['涨跌额'] = ths_df['增长值']
                
                expected_cols = ['开盘价', '最高价', '最低价', '昨收', '成交量', '成交额']
                for col in expected_cols:
                    standardized_df[col] = 0.0 # 使用浮点数填充
                
                temp_df = standardized_df
                temp_df.set_index('代码', inplace=True)
                
            else:
                app.logger.warning("备用数据源 [同花顺] 也返回了空数据。")
                
        except Exception as ths_e:
            app.logger.error(f"尝试备用数据源 [同花顺] 时也发生严重错误: {ths_e}")

    # --- 统一更新缓存 ---
    if temp_df is not None and not temp_df.empty:
        with cache_lock:
            etf_cache_df = temp_df
        app.logger.info(f"ETF行情缓存更新成功！当前缓存共 {len(etf_cache_df)} 条数据。")
    else:
        app.logger.error("所有数据源均获取ETF数据失败，本次缓存未更新。")

def clear_stock_cache():
    """清空股票缓存，为新交易日做准备。"""
    global stock_cache
    with cache_lock:
        if stock_cache:
            app.logger.info(f"清空旧的股票缓存，共 {len(stock_cache)} 条数据。")
            stock_cache = {}

def run_schedule():
    """后台调度任务的循环。"""
    app.logger.info("后台调度线程已启动。")
    schedule.every().day.at("09:25", "Asia/Shanghai").do(clear_stock_cache)
    schedule.every().day.at("15:05", "Asia/Shanghai").do(update_etf_cache)
    schedule.every(10).minutes.do(lambda: is_trade_time() and update_etf_cache())

    while True:
        schedule.run_pending()
        time.sleep(1)

# --- API 端点 ---
@app.route('/api/quotes', methods=['GET'])
def get_quotes():
    """
    【性能优化版】获取实时行情数据（支持股票和ETF）。
    通过单次API调用获取所有股票行情，避免高并发请求。
    """
    symbols_str = request.args.get('symbols')
    if not symbols_str:
        return jsonify({"error": "请提供'symbols'查询参数"}), 400

    symbol_list = [symbol.strip() for symbol in symbols_str.split(',')]
    results = {}
    trade_time_now = is_trade_time()

    # --- 1. 分离ETF和股票代码 ---
    etf_symbols = []
    stock_symbols = []
    for symbol in symbol_list:
        code_only = symbol.replace('sh', '').replace('sz', '')
        if code_only.startswith(('51', '56', '58', '15')):
            etf_symbols.append(symbol)
        else:
            stock_symbols.append(symbol)
    
    # --- 2. 处理ETF (从缓存读取) ---
    with cache_lock:
        current_etf_cache = etf_cache_df.copy() if etf_cache_df is not None else None
    
    for symbol in etf_symbols:
        code_only = symbol.replace('sh', '').replace('sz', '')
        try:
            if current_etf_cache is not None and code_only in current_etf_cache.index:
                results[symbol] = {"status": "success", "data": current_etf_cache.loc[code_only].to_dict()}
            else:
                raise ValueError("在ETF缓存中未找到该代码")
        except Exception as e:
            app.logger.error(f"处理ETF代码 {symbol} 时发生错误: {e}")
            results[symbol] = {"status": "error", "message": str(e)}

    # --- 3. 批量处理股票 (核心优化) ---
    if stock_symbols:
        stocks_to_fetch = []
        # 非交易时间，优先从收盘价缓存读取
        if not trade_time_now:
            with cache_lock:
                for symbol in stock_symbols:
                    if symbol in stock_cache:
                        app.logger.info(f"[{symbol}] 命中股票收盘价缓存。")
                        results[symbol] = {"status": "success", "data": stock_cache[symbol]}
                    else:
                        stocks_to_fetch.append(symbol) # 缓存未命中，加入待获取列表
        else:
            stocks_to_fetch = stock_symbols # 交易时间，全部实时获取

        # 如果有需要实时获取的股票
        if stocks_to_fetch:
            try:
                app.logger.info(f"准备批量获取 {len(stocks_to_fetch)} 只股票的实时行情...")
                # 【核心】一次性获取所有A股的实时行情
                all_stocks_df = ak.stock_zh_a_spot_em()
                all_stocks_df.set_index('代码', inplace=True)
                
                for symbol in stocks_to_fetch:
                    code_only = symbol.replace('sh', '').replace('sz', '')
                    if code_only in all_stocks_df.index:
                        quote_data = all_stocks_df.loc[code_only].to_dict()
                        results[symbol] = {"status": "success", "data": quote_data}
                        # 如果是非交易时间，将新获取的数据存入缓存
                        if not trade_time_now:
                            with cache_lock:
                                stock_cache[symbol] = quote_data
                    else:
                        results[symbol] = {"status": "error", "message": "未找到该股票代码的行情"}
            except Exception as e:
                app.logger.error(f"批量获取股票行情时发生错误: {e}")
                # 如果批量获取失败，为所有待处理的股票返回错误信息
                for symbol in stocks_to_fetch:
                    results[symbol] = {"status": "error", "message": f"批量获取失败: {e}"}

    return jsonify(results)


# --- 升级后的历史行情接口 (支持股票与ETF) ---
@app.route('/api/history', methods=['GET'])
def get_history():
    """
    获取单只股票或ETF的历史日K线数据。
    API内部会自动判断代码类型并调用相应的akshare函数。
    """
    symbol = request.args.get('symbol')
    start_date = request.args.get('start_date', '20200101') # 默认开始日期
    end_date = request.args.get('end_date', datetime.now().strftime('%Y%m%d')) # 默认结束日期为今天

    if not symbol:
        return jsonify({"status": "error", "message": "必须提供 'symbol' 参数。"}), 400

    try:
        code_only = symbol[-6:]
        app.logger.info(f"请求历史行情: symbol={symbol} (代码: {code_only}), start={start_date}, end={end_date}")

        history_df = None
        
        if code_only.startswith(('51', '56', '58', '15')):
            app.logger.info(f"代码 {code_only} 被识别为ETF，调用 fund_etf_hist_em。")
            history_df = ak.fund_etf_hist_em(symbol=code_only, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        else:
            app.logger.info(f"代码 {code_only} 被识别为股票，调用 stock_zh_a_hist。")
            history_df = ak.stock_zh_a_hist(symbol=code_only, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")

        if history_df is None or history_df.empty:
            raise ValueError(f"Akshare 未返回有效数据。请检查代码 '{symbol}' 和日期范围是否正确。")

        history_df['日期'] = history_df['日期'].astype(str)
        history_data = history_df.to_dict(orient='records')

        return jsonify({"status": "success", "symbol": symbol, "data": history_data})

    except Exception as e:
        app.logger.error(f"获取历史行情 {symbol} 时发生错误: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# --- 主程序入口 ---
if __name__ == '__main__':
    app.logger.info("服务器以开发模式启动，开始初始化...")
    update_etf_cache()
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=5000, debug=False)

elif __name__ != '__main__':
    app.logger.info("服务器由Gunicorn启动，开始初始化...")
    update_etf_cache()
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.daemon = True
    scheduler_thread.start()
