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
            # 主动触发切换逻辑
            raise ValueError("Empty data from primary source")
        else:
            # 主数据源成功，直接设置索引
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
                # 这是关键一步，确保后续代码能统一处理
                column_mapping = {
                    '基金代码': '代码',
                    '基金名称': '名称',
                    '当前-单位净值': '最新价',
                    '增长率': '涨跌幅',
                    '增长值': '涨跌额',
                    # 根据需要添加更多映射
                    # '成交量': '成交量',  # 假设同花顺接口有成交量
                    # '成交额': '成交额',  # 假设同花顺接口有成交额
                }
                
                # 创建一个新的DataFrame，只包含我们需要的、且重命名后的列
                standardized_df = pd.DataFrame()
                standardized_df['代码'] = ths_df['基金代码']
                standardized_df['名称'] = ths_df['基金名称']
                standardized_df['最新价'] = ths_df['当前-单位净值']
                # 东方财富的涨跌幅是数字，同花顺是百分比字符串，需要转换
                standardized_df['涨跌幅'] = pd.to_numeric(ths_df['增长率'], errors='coerce')
                standardized_df['涨跌额'] = ths_df['增长值']
                
                # Akshare的em源通常包含更多字段，如'成交量', '成交额'等
                # 同花顺源没有这些字段，我们用0或NaN填充以保证数据结构一致性
                # 这一步可以根据你的业务需求决定是否需要
                expected_cols = ['名称', '最新价', '涨跌额', '涨跌幅', '开盘价', '最高价', '最低价', '昨收', '成交量', '成交额']
                for col in expected_cols:
                    if col not in standardized_df.columns and col not in ['代码']: # 代码已经是索引
                         standardized_df[col] = 0 # 或者 float('nan')
                
                temp_df = standardized_df
                temp_df.set_index('代码', inplace=True)
                
            else:
                app.logger.warning("备用数据源 [同花顺] 也返回了空数据。")
                
        except Exception as ths_e:
            app.logger.error(f"尝试备用数据源 [同花顺] 时也发生严重错误: {ths_e}")

    # --- 无论来自哪个数据源，最后统一更新缓存 ---
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
        if stock_cache: # 只有在缓存不为空时才打印日志
            app.logger.info(f"清空旧的股票缓存，共 {len(stock_cache)} 条数据。")
            stock_cache = {}

def run_schedule():
    """后台调度任务的循环。"""
    app.logger.info("后台调度线程已启动。")
    # --- 定义调度任务 ---
    schedule.every().day.at("09:25", "Asia/Shanghai").do(clear_stock_cache)
    schedule.every().day.at("15:05", "Asia/Shanghai").do(update_etf_cache)
    # 交易时段内更频繁更新
    schedule.every(10).minutes.do(lambda: is_trade_time() and update_etf_cache())

    while True:
        schedule.run_pending()
        time.sleep(1)

# --- API 端点 ---
@app.route('/api/quotes', methods=['GET'])
def get_quotes():
    """获取实时行情数据（支持股票和ETF）"""
    symbols_str = request.args.get('symbols')
    if not symbols_str:
        return jsonify({"error": "请提供'symbols'查询参数"}), 400

    symbol_list = [symbol.strip() for symbol in symbols_str.split(',')]
    results = {}
    trade_time_now = is_trade_time()

    with cache_lock:
        current_etf_cache = etf_cache_df.copy() if etf_cache_df is not None else None

    for symbol in symbol_list:
        try:
            code_only = symbol.replace('sh', '').replace('sz', '')
            
            if code_only.startswith(('51', '56', '58', '15')):
                # 处理ETF
                if current_etf_cache is not None and code_only in current_etf_cache.index:
                    results[symbol] = {"status": "success", "data": current_etf_cache.loc[code_only].to_dict()}
                else:
                    raise ValueError("在ETF缓存中未找到该代码")
            else:
                # 处理股票
                if not trade_time_now and symbol in stock_cache:
                    app.logger.info(f"[{symbol}] 命中股票收盘价缓存。")
                    results[symbol] = {"status": "success", "data": stock_cache[symbol]}
                else:
                    app.logger.info(f"[{symbol}] {'交易时间' if trade_time_now else '非交易时间缓存未命中'}，实时请求...")
                    stock_df = ak.stock_bid_ask_em(symbol=symbol) # 注意：这里用完整的symbol
                    if stock_df is None or stock_df.empty:
                        raise ValueError("stock_bid_ask_em 返回空数据")
                    
                    quote_data = stock_df.set_index('item')['value'].to_dict()
                    results[symbol] = {"status": "success", "data": quote_data}
                    
                    if not trade_time_now:
                        with cache_lock:
                            stock_cache[symbol] = quote_data
        except Exception as e:
            app.logger.error(f"处理代码 {symbol} 时发生错误: {e}")
            results[symbol] = {"status": "error", "message": str(e)}

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
        # 提取6位纯数字代码
        code_only = symbol[-6:]
        app.logger.info(f"请求历史行情: symbol={symbol} (代码: {code_only}), start={start_date}, end={end_date}")

        history_df = None
        
        # --- 核心修改：智能路由，根据代码判断是股票还是ETF ---
        if code_only.startswith(('51', '56', '58', '15')):
            # 判断为ETF，调用ETF的历史行情接口
            app.logger.info(f"代码 {code_only} 被识别为ETF，调用 fund_etf_hist_em。")
            history_df = ak.fund_etf_hist_em(symbol=code_only,
                                             period="daily",
                                             start_date=start_date,
                                             end_date=end_date,
                                             adjust="qfq")
        else:
            # 默认作为股票处理
            app.logger.info(f"代码 {code_only} 被识别为股票，调用 stock_zh_a_hist。")
            history_df = ak.stock_zh_a_hist(symbol=code_only,
                                            period="daily",
                                            start_date=start_date,
                                            end_date=end_date,
                                            adjust="qfq")

        if history_df is None or history_df.empty:
            raise ValueError(f"Akshare 未返回有效数据。请检查代码 '{symbol}' 和日期范围是否正确。")

        # 将DataFrame转换为适合JSON的格式
        history_df['日期'] = history_df['日期'].astype(str)
        history_data = history_df.to_dict(orient='records')

        return jsonify({
            "status": "success",
            "symbol": symbol,
            "data": history_data
        })

    except Exception as e:
        app.logger.error(f"获取历史行情 {symbol} 时发生错误: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# --- 主程序入口 ---
if __name__ == '__main__':
    # 开发模式下直接运行时执行
    app.logger.info("服务器以开发模式启动，开始初始化...")
    update_etf_cache()
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(host='0.0.0.0', port=5000, debug=False)

elif __name__ != '__main__':
    # 当由Gunicorn等WSGI服务器启动时执行
    app.logger.info("服务器由Gunicorn启动，开始初始化...")
    update_etf_cache()
    scheduler_thread = threading.Thread(target=run_schedule)
    scheduler_thread.daemon = True
    scheduler_thread.start()
