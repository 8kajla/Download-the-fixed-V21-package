from __future__ import annotations
import logging, os, signal, time, shutil
from pathlib import Path
from collections import defaultdict

from state_store import StateStore
from market_discovery import discover, book, resolve
from paper_ledger import PaperLedger
from research_logger import ResearchLogger
from strategy import CapitalFirstStrategy
from execution_simulator import MarketFeed, RestingOrderSimulator

logging.basicConfig(level=logging.INFO,format='%(asctime)s UTC %(levelname)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
log=logging.getLogger('v21')

DATA=Path(os.getenv('DATA_DIR','/app/data')).expanduser()
if str(DATA) in ('/','.') or not DATA.is_absolute(): raise RuntimeError(f'Unsafe DATA_DIR: {DATA}')
DATA.mkdir(parents=True,exist_ok=True)
if os.getenv('PAPER_TRADING','true').lower() not in ('1','true','yes','on'): raise SystemExit('SAFETY LOCK: PAPER_TRADING must be true')
if os.getenv('FRESH_START','false').lower() in ('1','true','yes','on'):
    for p in DATA.iterdir(): shutil.rmtree(p) if p.is_dir() else p.unlink()

store=StateStore(DATA/'paper.db')
ledger=PaperLedger(store,float(os.getenv('STARTING_CAPITAL','1000')))
strategy=CapitalFirstStrategy(bankroll=ledger.initial_cash,max_total_exposure=float(os.getenv('MAX_TOTAL_EXPOSURE','300')),start_sec=float(os.getenv('START_TRADING_SECOND','0')),stop_sec=float(os.getenv('STOP_TRADING_SECOND','240')),hard_cutoff_seconds=float(os.getenv('HARD_CUTOFF_SECONDS','60')),min_trade_gap_seconds=float(os.getenv('MIN_TRADE_GAP_SECONDS','0')))
strategy.restore_policy_state(store.get_orders())

# Fail fast on internally inconsistent execution configuration.
_latency_ms=float(os.getenv('PAPER_ORDER_LATENCY_MS','150'))
_ttl=float(os.getenv('PAPER_ORDER_TTL_SECONDS','20'))
if _latency_ms < 0 or _ttl <= 0 or _ttl <= _latency_ms/1000.0:
    raise RuntimeError('PAPER_ORDER_TTL_SECONDS must be greater than PAPER_ORDER_LATENCY_MS/1000')
if float(os.getenv('PAPER_MIN_FILL_SHARES','0.0001')) <= 0:
    raise RuntimeError('PAPER_MIN_FILL_SHARES must be > 0')
feed=MarketFeed(store); execution=RestingOrderSimulator(feed,store,latency_ms=_latency_ms,ttl=_ttl,queue_safety=float(os.getenv('PAPER_QUEUE_SAFETY_FACTOR','1.0')),min_fill_shares=float(os.getenv('PAPER_MIN_FILL_SHARES','0.0001')))
research=ResearchLogger(DATA,ledger)
markets={}; histories=defaultdict(lambda:{'Up':[],'Down':[]});
# Recover market metadata from durable order/fill records so a discovery outage cannot
# orphan lifecycle handling after restart.
for _o in store.get_orders():
    _m=(_o.get('meta') or {}).get('market')
    if isinstance(_m,dict) and _o.get('condition'):
        markets.setdefault(str(_o['condition']),_m)
for _f in store.get_fills():
    _m=_f.get('market')
    if isinstance(_m,dict) and _f.get('condition'):
        markets.setdefault(str(_f['condition']),_m)
last_books={}; last_discovery=0.0; last_report=0.0; last_resolve=0.0; last_maintenance=0.0; next_trade_at=0.0; stopping=False


def shutdown(*_):
    global stopping
    stopping=True
    # Signal handler only flips the stop flag. The main thread owns network and
    # SQLite shutdown so it cannot race a report or an in-flight transaction.


signal.signal(signal.SIGTERM,shutdown); signal.signal(signal.SIGINT,shutdown)

def market_exposure(condition): return ledger.exposure(condition)

def reserved_exposure(): return sum(max(0.0,float(o.get('remaining_shares',0)) * float(o.get('price',0))) for o in execution.active_orders())
def global_exposure(): return ledger.total_open_cost() + reserved_exposure()

def append_history(condition,side,ts,bid):
    if bid is None:return
    h=histories[condition][side]; h.append((float(ts),float(bid))); cutoff=float(ts)-60
    histories[condition][side]=[x for x in h if x[0]>=cutoff]

def resolve_finished(now):
    for condition,m in list(markets.items()):
        if now < m['end_ts']+2:continue
        try:
            token,outcome,status=resolve(m)
        except Exception as exc:
            log.warning('RESOLVE_ERROR | asset=%s | slug=%s | %s',m.get('asset'),m.get('slug'),exc)
            continue
        if token:
            closed=ledger.settle(condition,token,now)
            if closed:log.info('RESOLVED | asset=%s | slug=%s | winner=%s | settled=%d',m['asset'],m['slug'],outcome,len(closed))
            markets.pop(condition,None); histories.pop(condition,None); last_books.pop(m.get('up'),None); last_books.pop(m.get('down'),None); strategy.forget_condition(condition)
            continue
        if status=='PENDING':continue
        # A closed market without a winner is never guessed; keep it visible for
        # diagnostics but cancel all resting orders so no post-end fills occur.
        execution.cancel_all_for_condition(condition,'MARKET_END',now)

def report():
    marks={token:float(px) for token,px in last_books.items()}
    r=ledger.mark(marks); ledger.accounting_audit(); log.info('P&L | cash=$%.2f | open=$%.2f | equity=$%.2f | pnl=$%.2f | realized=$%.2f | unrealized=$%.2f | positions=%d | ws_connected=%s | ws_messages=%d | ws_trades=%d',r['cash'],r['open_cost'],r['equity'],r['pnl'],r['realized'],r['unrealized'],len(ledger.positions),feed.connected,feed.message_count,feed.trade_count)

def main():
    global markets,last_discovery,last_report,last_resolve,last_maintenance,next_trade_at
    feed.start(); log.info('START V21 | paper_only=true | db=%s',DATA/'paper.db')
    loop=float(os.getenv('LOOP_SECONDS','1')); discovery_interval=10.0; resolve_interval=5.0; report_interval=float(os.getenv('REPORT_INTERVAL_SECONDS','60')); maintenance_interval=float(os.getenv('DATA_MAINTENANCE_SECONDS','3600')); 
    while not stopping:
        cycle=time.time()
        try:
            now=time.time()
            if now-last_discovery>=discovery_interval:
                found=discover(now=now,lookahead=600)
                discovered={m['condition']:m for m in found}
                # Never discard a market that still has an open position/order merely
                # because discovery temporarily failed. Keep it until lifecycle closes.
                for condition,m in discovered.items(): markets[condition]=m
                live_conditions={o['condition'] for o in execution.active_orders()} | {p['condition_id'] for p in ledger.positions.values() if float(p.get('shares',0))>1e-12}
                for condition,m in list(markets.items()):
                    if condition not in discovered and condition not in live_conditions and now > float(m['end_ts']) + 120:
                        markets.pop(condition,None); histories.pop(condition,None); last_books.pop(m['up'],None); last_books.pop(m['down'],None); strategy.forget_condition(condition)
                last_discovery=now
                feed.set_assets([x for m in markets.values() if now <= float(m['end_ts'])+120 for x in (m['up'],m['down'])])
                log.info('CLOB_WS | connected=%s | messages=%d | trades=%d | books=%d | price_changes=%d | last_trade_at=%.3f',feed.connected,feed.message_count,feed.trade_count,feed.book_count,feed.price_change_count,feed.last_trade_at)
            # First process durable public events, then lifecycle cancellation.
            fills=execution.process(now)
            if fills: log.info('EXECUTION | fills=%d',len(fills))
            if fills: ledger._refresh()
            for f in fills:
                log.info('FILL PAPER | order=%s | %s | %.6f sh @ %.4f | public=%.4f x %.6f',f['order_id'][:14],f['side'],f['shares'],f['price'],f['trade_price'],f['trade_size'])
            for o in list(execution.orders.values()):
                if o['status']=='RESTING' and now>=float(o['expires_ts']): execution.cancel(o['order_id'],'EXPIRED',now)
                end=float((o.get('meta') or {}).get('end_ts') or 0)
                if o['status']=='RESTING' and end and now>=end: execution.cancel(o['order_id'],'MARKET_END',now)
            if now>=next_trade_at:
                # REST snapshots are used for strategy state; they do not imply fills.
                candidates=[]; book_cache={}
                for m in list(markets.values()):
                    elapsed=now-m['start_ts']; left=m['end_ts']-now
                    if elapsed<strategy.start_sec or elapsed>=strategy.stop_sec or left<=strategy.hard_cutoff_seconds or not m['accepting_orders']:continue
                    try:
                        ub,ua,ud,uad=book(m['up']); db,da,dd,dad=book(m['down'])
                    except Exception as exc:
                        log.warning('BOOK_ERROR | asset=%s | slug=%s | %s',m['asset'],m['slug'],exc); continue
                    book_cache[m['condition']]={'up':(ub,ua,ud,uad),'down':(db,da,dd,dad)}; last_books[m['up']]=ub if ub is not None else last_books.get(m['up'],0.5); last_books[m['down']]=db if db is not None else last_books.get(m['down'],0.5)
                    append_history(m['condition'],'Up',now,ub); append_history(m['condition'],'Down',now,db)
                    ec=strategy.market_entry_count(m['condition']); prev=strategy.market_last_side.get(m['condition'])
                    candidates.extend(strategy.build_candidates_for_market(elapsed,ua,da,ub,db,histories[m['condition']]['Up'],histories[m['condition']]['Down'],now,asset=m['asset'],market=m['asset'],market_entry_count=ec,up_depth=ud,down_depth=dd,previous_side=prev,condition=m['condition'],seconds_since_first_entry=(now-strategy.market_first_signal.get(m['condition'],now))))
                if candidates and feed.connected and (now-feed.last_message_at)<=15:
                    target=strategy.choose_distribution_band(candidates); chosen=strategy.choose_process_candidate(candidates,target)
                    if chosen:
                        # Find market by token; all candidate fields are sourced from a current snapshot.
                        # Candidate identity is represented by asset/side; resolve to the current market snapshot.
                        cm=markets.get(str(chosen.get('condition') or ''))
                        if cm is None:
                            cm=next((m for m in markets.values() if m['asset'].upper()==str(chosen['asset']).upper() and ((chosen['side']=='Up' and m['up']==chosen.get('token')) or (chosen['side']=='Down' and m['down']==chosen.get('token')))),None)
                        if cm:
                            token=cm['up'] if chosen['side']=='Up' else cm['down']
                            if execution.token_fresh(token,now,15.0):
                                notion=min(float(chosen['target']),max(0.0,ledger.cash-reserved_exposure()),max(0.0,strategy.max_total_exposure-global_exposure()))
                                if notion>=float(os.getenv('MIN_PAPER_FILL_USD','0.01')):
                                    shares=notion/chosen['bid']; rest_depth=book_cache[cm['condition']]['up'][2] if chosen['side']=='Up' else book_cache[cm['condition']]['down'][2]; depth=execution.feed.bid_depth(token,chosen['bid']); depth=rest_depth if depth is None else depth
                                    meta={'asset':cm['asset'],'market':cm,'entry_count_before':strategy.market_entry_count(cm['condition']),'burst_position':int(chosen.get('burst_position',0)),'trajectory_likelihood':chosen['trajectory_likelihood'],'reason':chosen['reason'],'strategy_band':chosen['band'],'strategy_notional':notion}
                                    o=execution.submit_buy(cm['condition'],token,cm,chosen['side'],chosen['bid'],shares,now,queue_hint=depth,meta=meta)
                                    strategy.observe_signal(cm['condition'],chosen['band'],notion,now,chosen['side']); next_trade_at=now+max(strategy.min_trade_gap_seconds,strategy.cadence.sample_gap()); log.info('ORDER RESTING | V21 REALISTIC CLOB | asset=%s | side=%s | notional=$%.4f | bid=$%.4f | shares=%.6f | queue_ahead=%.6f | ttl=%.1fs | latency=%.3fs',cm['asset'],chosen['side'],notion,chosen['bid'],shares,o['queue_ahead'],execution.ttl,execution.latency_ms/1000)
                                else:
                                    log.info('SIGNAL_REJECT | reason=NOTIONAL_BELOW_MIN | asset=%s | side=%s | target=$%.4f | available=$%.4f',cm['asset'],chosen['side'],float(chosen['target']),notion)
                            else:
                                log.info('SIGNAL_REJECT | reason=TOKEN_NOT_FRESH | asset=%s | side=%s | token=%s | ws_age=%.2f | book_age=%.2f',cm['asset'],chosen['side'],token,now-feed.last_message_by_token.get(token,0.0),now-feed.last_book_by_token.get(token,0.0))
            else:
                if not candidates:
                    log.info('SIGNAL_REJECT | reason=NO_CANDIDATES | markets=%d',len(markets))
                elif not feed.connected or (now-feed.last_message_at)>15:
                    log.warning('EXECUTION_GUARD | no fresh CLOB websocket; no new orders')
            
            if now-last_resolve>=resolve_interval:resolve_finished(now); last_resolve=now
            if now-last_maintenance>=maintenance_interval:
                research.maintenance()
                last_maintenance=now
            if now-last_report>=report_interval:report(); last_report=now
            time.sleep(max(0.05,loop-(time.time()-cycle)))
        except Exception as exc:
            log.exception('LOOP_ERROR | %s',exc)
            time.sleep(min(5.0,max(0.5,loop)))
    try:
        report()
    finally:
        feed.stop(); store.close()

if __name__=='__main__': main()
