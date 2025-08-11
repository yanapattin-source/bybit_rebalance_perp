"""
bybit_rebalance_bot.py
Rebalance Fix Asset Value Strategy with Margin Checks, Ledger-based Realized PnL & Funding Accounting
Uses CCXT with Bybit perpetual futures (linear swaps)

Requirements:
- Install ccxt (`pip install ccxt`)
- Set BYBIT_API_KEY and BYBIT_API_SECRET environment variables for real trading
- Set TESTNET=True for Bybit testnet
"""

import os
import time
import math
import csv
import traceback
import ccxt

# --------- CONFIG ---------
API_KEY    = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
TESTNET    = True   # True=testnet; False=mainnet
DRY_RUN    = True   # True=no orders placed
SYMBOL_CCXT = "BTC/USDT"
MARGIN_CCY = SYMBOL_CCXT.split("/")[1]  # currency used for margin and ledger queries
TIMEFRAME = "1m"         # Timeframe for OHLCV, ATR, EMA calculations (e.g. "1m", "5m", "15m")
TARGET_NOTIONAL = 3000.0      # Target quote exposure in USDT
LEVERAGE = 3                  # Leverage to use
BASE_TOL_PCT = 1.0            # Base tolerance % before rebalance triggers
ATR_LEN = 14
VOL_REF_ATR_PCT = 1.0
VOL_SCALE_MIN = 0.5
VOL_SCALE_MAX = 3.0
EMA_SHORT_LEN = 10
EMA_LONG_LEN = 50
TREND_STRENGTH_MULT = 1.0
QTY_STEP = 0.0001
MIN_TRADE_VALUE = 10.0        # Minimum trade value in USDT
ALLOW_SHORT = False
USE_MARKET = True
LOOP_INTERVAL_SEC = 60
MAX_ORDER_RETRIES = 3

CSV_LOG_FILE = "bybit_rebalance_log.csv"

# --------------------------

# Initialize exchange
exchange_class = ccxt.bybit
exchange = exchange_class({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "enableRateLimit": True,
    "options": {
        "defaultType": "future",
        "adjustForTimeDifference": True,
    }
})
if TESTNET:
    exchange.set_sandbox_mode(True)

# Helper: write CSV header if file not exists
if not os.path.exists(CSV_LOG_FILE):
    with open(CSV_LOG_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "action", "side", "qty", "price",
            "realized_pnl", "funding_paid", "fees_paid",
            "pos_qty", "pos_notional", "equity"
        ])

# --- Ledger PnL Tracker ---
class LedgerPnLTracker:
    def __init__(self):
        self.seen_ids = set()
        self.realized = 0.0
        self.funding_paid = 0.0
        self.fees_paid = 0.0

    def ingest_ledger_entry(self, entry):
        eid = entry.get('id')
        if eid in self.seen_ids:
            return
        self.seen_ids.add(eid)

        entry_type = entry.get('type', '').lower()
        amount = float(entry.get('amount', 0.0))

        if entry_type in ['realized_pnl', 'pnl', 'profit_and_loss', 'settlement']:
            self.realized += amount
        elif entry_type == 'funding_fee':
            self.funding_paid += amount
        elif entry_type in ['trade_fee', 'fee']:
            self.fees_paid += abs(amount)

    def ingest_ledger_batch(self, ledger):
        for e in ledger:
            self.ingest_ledger_entry(e)

    def get_realized(self):
        return self.realized

    def get_funding_paid(self):
        return self.funding_paid

    def get_fees_paid(self):
        return self.fees_paid

ledger_tracker = LedgerPnLTracker()

# --- Utility Functions ---
def fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=100):
    try:
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        print(f"Error fetching OHLCV: {e}")
        return []

def calculate_atr(ohlcvs, length):
    if len(ohlcvs) < length + 1:
        return None
    trs = []
    for i in range(1, length + 1):
        high = ohlcvs[-i][2]
        low = ohlcvs[-i][3]
        close_prev = ohlcvs[-i-1][4]
        tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
        trs.append(tr)
    return sum(trs) / length

def calculate_ema(values, length):
    if len(values) < length:
        return None
    k = 2 / (length + 1)
    ema = sum(values[:length]) / length
    for price in values[length:]:
        ema = price * k + ema * (1 - k)
    return ema

def get_price():
    ticker = exchange.fetch_ticker(SYMBOL_CCXT)
    return ticker['last']

def get_position():
    try:
        positions = exchange.fetch_positions([SYMBOL_CCXT])
        for pos in positions:
            if pos['symbol'] == SYMBOL_CCXT:
                qty = pos['contracts'] if pos['contracts'] is not None else 0.0
                side = pos['side'] if pos['side'] is not None else ''
                entry_price = pos.get('entryPrice', 0.0) or 0.0
                return {
                    "qty": qty,
                    "side": side,
                    "entry_price": float(entry_price)
                }
    except Exception as e:
        print(f"Error fetching position: {e}")
    return {"qty": 0.0, "side": "", "entry_price": 0.0}

def get_margin_balance():
    try:
        balance = exchange.fetch_balance()
        usdt_free = 0.0
        if 'USDT' in balance['free']:
            usdt_free = float(balance['free']['USDT'])
        elif 'USDT' in balance:
            usdt_free = float(balance['USDT']['free'])
        else:
            usdt_free = sum(float(v) for k,v in balance['free'].items() if 'USDT' in k)
        return usdt_free
    except Exception as e:
        print(f"Error fetching margin balance: {e}")
        return 0.0

def check_order_margin(side, qty, price):
    available_margin = get_margin_balance()
    margin_needed = (qty * price) / LEVERAGE
    if margin_needed > available_margin:
        print(f"Insufficient margin for order: need {margin_needed:.4f} USDT, have {available_margin:.4f} USDT")
        return False
    return True

def round_qty(qty):
    return math.floor(qty / QTY_STEP) * QTY_STEP

# --- Main rebalance function ---
def compute_and_maybe_rebalance():
    try:
        # Get recent OHLCV for ATR and EMA
        ohlcvs = fetch_ohlcv(SYMBOL_CCXT, timeframe=TIMEFRAME, limit=max(EMA_LONG_LEN+ATR_LEN+5, 100))
        closes = [candle[4] for candle in ohlcvs]

        price = get_price()
        if price is None:
            print("Price unavailable; skipping iteration.")
            return

        atr = calculate_atr(ohlcvs, ATR_LEN)
        if atr is None:
            print("Not enough data for ATR; skipping iteration.")
            return
        atr_pct = (atr / price) * 100.0
        vol_scale_raw = atr_pct / VOL_REF_ATR_PCT
        vol_scale = max(VOL_SCALE_MIN, min(VOL_SCALE_MAX, vol_scale_raw))

        ema_short = calculate_ema(closes, EMA_SHORT_LEN)
        ema_long = calculate_ema(closes, EMA_LONG_LEN)
        if ema_short is None or ema_long is None:
            print("Not enough data for EMA; skipping iteration.")
            return

        trend_up = ema_short > ema_long
        trend_down = ema_short < ema_long
        ema_diff_pct = abs(ema_short - ema_long) / ema_long * 100.0
        trend_strength = ema_diff_pct * TREND_STRENGTH_MULT

        equity = get_margin_balance() * LEVERAGE
        target_notional = TARGET_NOTIONAL

        effective_tol_value = target_notional * (BASE_TOL_PCT / 100.0) * vol_scale

        pos = get_position()
        pos_qty = pos['qty'] if pos['side'].lower() == 'long' else -pos['qty'] if pos['side'].lower() == 'short' else 0.0
        pos_notional = abs(pos_qty) * price

        deviation = pos_notional - target_notional
        deviation_abs = abs(deviation)

        raw_qty_needed = deviation_abs / price
        rounded_qty = round_qty(raw_qty_needed)
        rounded_qty = max(0.0, rounded_qty)

        sell_scale = 1.0
        buy_scale = 1.0
        if trend_up:
            sell_scale = 1.0 / (1.0 + min(trend_strength, 100.0) / 10.0)
        if trend_down:
            buy_scale = 1.0 / (1.0 + min(trend_strength, 100.0) / 10.0)
        sell_scale = max(0.1, min(1.0, sell_scale))
        buy_scale = max(0.1, min(1.0, buy_scale))

        should_rebalance = (deviation_abs > effective_tol_value) and (rounded_qty * price >= MIN_TRADE_VALUE)
        final_side = ""
        final_qty = 0.0
        reduce_only = False

        if should_rebalance:
            if deviation > 0:
                if pos_qty > 0:
                    qty_after = rounded_qty * sell_scale
                    if not ALLOW_SHORT:
                        qty_after = min(qty_after, abs(pos_qty))
                    if qty_after > 0:
                        final_side = "sell"
                        final_qty = qty_after
                        reduce_only = True
                    else:
                        should_rebalance = False
                elif pos_qty < 0:
                    qty_after = rounded_qty * buy_scale
                    if not ALLOW_SHORT:
                        qty_after = min(qty_after, abs(pos_qty))
                    if qty_after > 0:
                        final_side = "buy"
                        final_qty = qty_after
                        reduce_only = True
                    else:
                        should_rebalance = False
                else:
                    should_rebalance = False
            else:
                qty_after = rounded_qty * buy_scale
                if qty_after > 0:
                    final_side = "buy"
                    final_qty = qty_after
                    reduce_only = False
                else:
                    should_rebalance = False

        final_notional = final_qty * price
        if final_notional < MIN_TRADE_VALUE:
            should_rebalance = False

        # Fetch ledger entries (last 24h) for PnL accounting
        try:
            since_ledger = int((time.time() - 86400) * 1000)
            # fetch ledger for the margin currency (e.g., USDT) rather than the trading symbol
            ledger_entries = exchange.fetch_ledger(MARGIN_CCY, since=since_ledger, limit=200)
            ledger_tracker.ingest_ledger_batch(ledger_entries)
        except Exception as e:
            print("Error fetching ledger entries:", e)

        realized_pnl_ledger = ledger_tracker.get_realized()
        funding_paid_ledger = ledger_tracker.get_funding_paid()
        fees_paid_ledger = ledger_tracker.get_fees_paid()

        order_note = ""
        if should_rebalance and final_qty > 0:
            if DRY_RUN:
                print(f"[dry_run] Would place {final_side} order for {final_qty} contracts at approx price {price}")
                order_note = "dry_run"
            else:
                if not check_order_margin(final_side, final_qty, price):
                    print("Skipping order due to insufficient margin.")
                    order_note = "skip_insufficient_margin"
                else:
                    order_resp = None
                    for attempt in range(MAX_ORDER_RETRIES):
                        try:
                            params = {"reduceOnly": reduce_only}
                            if USE_MARKET:
                                if final_side == "buy":
                                    order_resp = exchange.create_market_buy_order(SYMBOL_CCXT, final_qty, params)
                                else:
                                    order_resp = exchange.create_market_sell_order(SYMBOL_CCXT, final_qty, params)
                            else:
                                if final_side == "buy":
                                    order_resp = exchange.create_limit_buy_order(SYMBOL_CCXT, final_qty, price, params)
                                else:
                                    order_resp = exchange.create_limit_sell_order(SYMBOL_CCXT, final_qty, price, params)
                            break
                        except Exception as e:
                            print(f"Order attempt {attempt+1} failed: {e}")
                            traceback.print_exc()
                            time.sleep(1)
                    if order_resp:
                        print("Order placed:", order_resp)
                        order_note = "order_placed"
                    else:
                        order_note = "order_failed"
        else:
            order_note = "no_action"

        log_row = [
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            order_note,
            final_side if final_side else "none",
            f"{final_qty:.6f}",
            f"{price:.2f}",
            f"{realized_pnl_ledger:.6f}",
            f"{funding_paid_ledger:.6f}",
            f"{fees_paid_ledger:.6f}",
            f"{pos_qty:.6f}",
            f"{pos_notional:.2f}",
            f"{equity:.2f}",
        ]
        with open(CSV_LOG_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(log_row)

    except Exception as e:
        print("Exception in compute_and_maybe_rebalance:", e)
        traceback.print_exc()

# --- Main loop ---
if __name__ == "__main__":
    print("Starting Bybit rebalance bot")
    print(f"Dry run: {DRY_RUN}, Testnet: {TESTNET}, Symbol: {SYMBOL_CCXT}, Timeframe: {TIMEFRAME}")
    try:
        if not DRY_RUN:
            try:
                print(f"Setting leverage {LEVERAGE}x on {SYMBOL_CCXT}")
                exchange.set_leverage(LEVERAGE, SYMBOL_CCXT)
            except Exception as e:
                print(f"Error setting leverage: {e}")

        while True:
            compute_and_maybe_rebalance()
            time.sleep(LOOP_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Stopping by user request")
    except Exception as e:
        print(f"Fatal error: {e}")
