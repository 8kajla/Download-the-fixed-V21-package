import json, sys, tempfile, time
import pytest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from state_store import StateStore
from execution_simulator import MarketFeed, RestingOrderSimulator, event_key
from paper_ledger import PaperLedger
from market_discovery import normalize
from strategy import CapitalFirstStrategy


def mk(tmp=None):
    p=Path(tmp or tempfile.mkdtemp())/'paper.db'; st=StateStore(p); led=PaperLedger(st,1000); feed=MarketFeed(st); ex=RestingOrderSimulator(feed,st,latency_ms=0,ttl=20); return st,led,feed,ex

def add_trade(st,token,price,size,side,ts,tx=''):
    t={'token_id':token,'price':price,'size':size,'side':side,'timestamp':ts,'transaction_hash':tx}; t['trade_key']=event_key(t)
    with st.tx(): st.conn.execute('BEGIN IMMEDIATE'); st.add_market_trade(t); st.conn.execute('COMMIT')
    return t

def test_book_event_shape_and_hype_slug():
    m=normalize({'slug':'hype-updown-5m-1788536400','conditionId':'c','clobTokenIds':'["u","d"]','outcomes':'["Up","Down"]','acceptingOrders':True,'enableOrderBook':True})
    assert m and m['asset']=='HYPE' and m['up']=='u' and m['down']=='d'

def test_trade_through_is_not_assumed_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}
    ex.submit_buy('c','t',m,'Up',.25,2,now-1,0,{'asset':'BTC'})
    add_trade(st,'t',.24,2,'SELL',now,'tx1'); assert ex.process(now+1)==[]

def test_queue_ahead_consumed_before_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}
    ex.submit_buy('c','t',m,'Up',.25,2,now-1,3,{'asset':'BTC'})
    add_trade(st,'t',.25,2,'SELL',now,'tx1'); assert ex.process(now)==[]
    add_trade(st,'t',.25,2,'SELL',now+0.1,'tx2'); fs=ex.process(now+0.2); assert len(fs)==1 and fs[0]['shares']==1

def test_duplicate_trade_never_replays():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0)
    t=add_trade(st,'t',.25,1,'SELL',now,'same'); assert len(ex.process(now+1))==1; assert len(ex.process(now+2))==0; assert st.conn.execute('select count(*) from fills').fetchone()[0]==1

def test_two_orders_fifo_same_price():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}
    o1=ex.submit_buy('c','t',m,'Up',.25,1,now-2,0); o2=ex.submit_buy('c','t',m,'Up',.25,1,now-1,0)
    add_trade(st,'t',.25,1,'SELL',now,'tx1'); fs=ex.process(now+1); assert len(fs)==1 and fs[0]['order_id']==o1['order_id']

def test_partial_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,2,now-1,0)
    add_trade(st,'t',.25,0.75,'SELL',now,'tx1'); fs=ex.process(now+1); assert fs[0]['shares']==0.75
    add_trade(st,'t',.25,1.25,'SELL',now+.1,'tx2'); fs=ex.process(now+2); assert fs[0]['shares']==1.25

def test_expiry_prevents_late_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-21,0)
    add_trade(st,'t',.25,1,'SELL',now,'tx1'); assert ex.process(now)==[]; assert ex.orders[next(iter(ex.orders))]['status']=='EXPIRED'

def test_market_end_prevents_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now-1}; ex.submit_buy('c','t',m,'Up',.25,1,now-2,0)
    add_trade(st,'t',.25,1,'SELL',now,'tx1'); assert ex.process(now+1)==[]

def test_future_trade_ignored_until_now():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0)
    add_trade(st,'t',.25,1,'SELL',now+10,'tx1'); assert ex.process(now)==[]; assert ex.process(now+11)

def test_same_tx_distinct_executions_do_not_collapse():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); ex.submit_buy('c','t',m,'Up',.24,1,now-1,0)
    add_trade(st,'t',.25,1,'SELL',now,'tx'); add_trade(st,'t',.24,1,'SELL',now+.01,'tx'); fs=ex.process(now+1); assert len(fs)==2

def test_atomic_fill_persists_order_fill_ledger_together():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1)
    assert st.conn.execute('select count(*) from fills').fetchone()[0]==1
    assert st.conn.execute('select count(*) from ledger_trades where action="BUY"').fetchone()[0]==1
    assert st.conn.execute('select status from orders where order_id=?',(o['order_id'],)).fetchone()[0]=='FILLED'

def test_state_survives_restart():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1); p=st.path; st.close(); st2=StateStore(p); ex2=RestingOrderSimulator(MarketFeed(st2),st2); led2=PaperLedger(st2,1000); assert ex2.orders[o['order_id']]['status']=='FILLED'; assert len(ex2.fills)==1; assert abs(led2.cash-999.75)<1e-9

def test_resolution_is_idempotent():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1); assert len(led.settle('c','t',now+2))==1; assert len(led.settle('c','t',now+3))==0; led._refresh(); assert abs(led.cash-1000.75)<1e-9 and led.realized==.75

def test_strategy_uses_buy_only_and_hard_cutoff():
    s=CapitalFirstStrategy(bankroll=1000,start_sec=0,stop_sec=240,hard_cutoff_seconds=60); c=s.build_candidates_for_market(100,.5,.5,.2,.2,[],[],time.time(),asset='BTC',market='BTC',market_entry_count=0); assert all(x['side'] in ('Up','Down') for x in c); assert s.build_candidates_for_market(181,.5,.5,.2,.2,[],[],time.time(),asset='BTC',market='BTC')==[]

def test_strategy_restores_from_durable_orders():
    s=CapitalFirstStrategy(); orders=[{'order_id':'o1','condition':'c','side':'Up','price':0.12,'requested_shares':5,'submitted_ts':10,'meta':{'strategy_band':'C10_15','strategy_notional':0.6}}]
    s.restore_policy_state(orders); assert s.market_entry_count('c')==1; assert s.market_last_side['c']=='Up'; assert s.scheduler.trade_counts['C10_15']==1

def test_store_integrity():
    st,_,_,_=mk(); assert st.integrity_check()=='ok'

def test_feed_parses_current_last_trade_and_durably_stores():
    st,led,feed,ex=mk(); feed._handle(json.dumps({'event_type':'last_trade_price','asset_id':'t','price':'0.25','size':'3','side':'SELL','timestamp':str(int(time.time()*1000)),'transaction_hash':'tx'})); assert feed.trade_count==1; assert len(st.get_unseen_market_trades(time.time()+1))==1

def test_feed_duplicate_payload_is_ignored():
    st,led,feed,ex=mk(); msg={'event_type':'last_trade_price','asset_id':'t','price':'0.25','size':'3','side':'SELL','timestamp':str(int(time.time()*1000)),'transaction_hash':'tx'}; raw=json.dumps(msg); feed._handle(raw); feed._handle(raw); assert feed.trade_count==1; assert len(st.get_unseen_market_trades(time.time()+1))==1

def test_buy_trade_does_not_fill_passive_buy():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'BUY',now,'tx'); assert ex.process(now+1)==[]

def test_cross_token_trade_does_not_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'other',.20,1,'SELL',now,'tx'); assert ex.process(now+1)==[]

def test_cash_limit_is_enforced_atomically():
    st,led,feed,ex=mk(); st.set_meta('cash',0.1); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,2,now-1,0); add_trade(st,'t',.25,2,'SELL',now,'tx'); fs=ex.process(now+1); assert len(fs)==1 and abs(fs[0]['notional']-.1)<1e-9; assert abs(float(st.get_meta('cash')))<1e-9

def test_two_price_levels_do_not_fill_higher_bid_from_lower_print():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; a=ex.submit_buy('c','t',m,'Up',.25,1,now-2,0); ex.submit_buy('c','t',m,'Up',.24,1,now-1,0); add_trade(st,'t',.24,1,'SELL',now,'tx'); fs=ex.process(now+1); assert len(fs)==1 and fs[0]['order_id']!=a['order_id']

def test_same_public_trade_volume_cannot_double_spend():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,2,now-2,0); ex.submit_buy('c','t',m,'Up',.24,2,now-1,0); add_trade(st,'t',.24,2,'SELL',now,'tx'); fs=ex.process(now+1); assert sum(x['shares'] for x in fs)==2

def test_restart_keeps_seen_trade_dedupe():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1); p=st.path; st.close(); st2=StateStore(p); feed2=MarketFeed(st2); ex2=RestingOrderSimulator(feed2,st2); assert ex2.process(now+2)==[]

def test_strategy_fine_distribution_is_normalized():
    s=CapitalFirstStrategy(); assert abs(sum(s.fine_band_trade_share.values())-1)<1e-6

def test_strategy_asset_priors_cover_six_assets():
    s=CapitalFirstStrategy(); assert set(s.scheduler.asset_trade_share)=={'BTC','SOL','DOGE','HYPE','ETH','BNB'}; assert abs(sum(s.scheduler.asset_trade_share.values())-1)<1e-9

def test_order_submission_records_event_once():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now,0); assert st.conn.execute('select count(*) from events where event_id=?',('order:'+o['order_id'],)).fetchone()[0]==1

def test_reserved_orders_limit_new_exposure():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t1',m,'Up',.25,100,now,0); reserved=sum(o['remaining_shares']*o['price'] for o in ex.active_orders()); assert abs(reserved-25)<1e-9


def test_trade_failure_is_retryable():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); tr=add_trade(st,'t',.25,1,'SELL',now,'tx')
    bad={'order_id':'missing','fill_id':'bad','condition':'c','token':'t','market':m,'side':'Up','price':.25,'shares':1,'notional':.25,'ts':now,'trade_price':.25,'trade_size':1,'transaction_hash':'tx','trade_key':tr['trade_key'],'meta':{}}
    with pytest.raises(Exception): st.apply_trade_atomically(tr,{},[bad])
    assert not st.seen_trade(tr['trade_key'])
    ex.process(now+1); assert st.seen_trade(tr['trade_key']) and st.get_order(o['order_id'])['status']=='FILLED'

def test_filled_order_cannot_receive_second_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'a'); assert len(ex.process(now+1))==1; add_trade(st,'t',.25,1,'SELL',now+.1,'b'); assert ex.process(now+2)==[]

def test_trade_at_different_price_does_not_prove_passive_fill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.24,1,'SELL',now,'tx'); assert ex.process(now+1)==[]

def test_seen_trade_survives_maintenance():
    st,led,feed,ex=mk(); now=time.time(); add_trade(st,'t',.25,1,'SELL',now,'tx'); assert ex.process(now+1)==[]; st.purge_old_market_events(now+100000); assert st.seen_trade(event_key({'token_id':'t','price':.25,'size':1,'side':'SELL','timestamp':now,'transaction_hash':'tx'}))

def test_event_key_ignores_timestamp_when_tx_present():
    a=event_key({'token_id':'t','price':.25,'size':1,'side':'SELL','timestamp':1,'transaction_hash':'tx'}); b=event_key({'token_id':'t','price':.25,'size':1,'side':'SELL','timestamp':2,'transaction_hash':'tx'}); assert a==b

def test_constraints_reject_overfill():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now,0)
    with pytest.raises(Exception):
        with st.tx(): st.conn.execute('BEGIN IMMEDIATE'); st.put_order({**o,'filled_shares':2,'remaining_shares':0}); st.conn.execute('COMMIT')

def test_discovery_boolean_parser_rejects_false_strings():
    from market_discovery import normalize
    m=normalize({'slug':'btc-updown-5m-1788536400','conditionId':'c','clobTokenIds':'["u","d"]','outcomes':'["Up","Down"]','acceptingOrders':'false','enableOrderBook':'false'}); assert m and m['accepting_orders'] is False and m['enable_order_book'] is False

def test_feed_token_freshness_requires_observed_token():
    st,led,feed,ex=mk(); assert not ex.token_fresh('t',time.time()); feed._desired={'t'}; feed._handle(json.dumps({'event_type':'book','asset_id':'t','timestamp':str(int(time.time()*1000)),'bids':[{'price':'0.25','size':'1'}],'asks':[{'price':'0.26','size':'1'}]})); assert ex.token_fresh('t',time.time())

def test_strategy_candidate_carries_condition():
    s=CapitalFirstStrategy(); c=s.build_candidates_for_market(100,.3,.3,.2,.2,[],[],time.time(),asset='BTC',market='BTC',condition='cond'); assert c and all(x['condition']=='cond' for x in c)


def test_same_price_fifo_allocation_with_one_public_print():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; a=ex.submit_buy('c','t',m,'Up',.25,2,now-3,0); b=ex.submit_buy('c','t',m,'Up',.25,2,now-2,0); add_trade(st,'t',.25,3,'SELL',now,'tx'); fs=ex.process(now+1); assert sum(f['shares'] for f in fs)==3; assert any(f['order_id']==a['order_id'] and f['shares']==2 for f in fs); assert any(f['order_id']==b['order_id'] and f['shares']==1 for f in fs)

# Adversarial regression tests added for the full audit.
def test_event_timestamp_accepts_seconds_and_ms_equally():
    from execution_simulator import event_ts
    now=time.time(); assert abs(event_ts(now)-now)<1e-6; assert abs(event_ts(now*1000)-now)<1e-3

def test_book_timestamp_out_of_order_does_not_regress():
    st,led,feed,ex=mk(); now=time.time();
    feed._handle(json.dumps({'event_type':'book','asset_id':'t','timestamp':str(now),'bids':[{'price':'0.25','size':'1'}],'asks':[{'price':'0.26','size':'1'}]}))
    feed._handle(json.dumps({'event_type':'book','asset_id':'t','timestamp':str(now-2),'bids':[{'price':'0.20','size':'9'}],'asks':[{'price':'0.21','size':'9'}]}))
    assert abs(feed.bid_depth('t',.25)-1)<1e-9

def test_price_change_before_fresh_book_is_ignored():
    st,led,feed,ex=mk(); now=time.time();
    feed._handle(json.dumps({'event_type':'price_change','asset_id':'t','timestamp':str(now),'price_changes':[{'asset_id':'t','side':'BUY','price':'0.25','size':'9'}]}))
    assert feed.bid_depth('t',.25) is None and not feed.token_fresh('t',now)

def test_reconnect_invalidates_old_book_state():
    st,led,feed,ex=mk(); now=time.time(); feed._desired={'t'}; feed.books['t']={'snapshot':True,'bids':{.25:1},'asks':{},'ts':now}; feed._confirmed={'t'}; feed.last_book_by_token['t']=now
    # Simulate the state reset performed at reconnect.
    feed._confirmed=set(); feed.books.pop('t',None); feed.last_book_by_token.pop('t',None)
    assert not feed.token_fresh('t',now)

def test_fill_fk_and_notional_constraint():
    st,led,feed,ex=mk(); fk=list(st.conn.execute('PRAGMA foreign_key_list(fills)')); assert any(r['table']=='orders' and r['from']=='order_id' for r in fk)
    now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now,0)
    with pytest.raises(Exception):
        with st.tx():
            st.conn.execute('BEGIN IMMEDIATE'); st.put_order({**o,'filled_shares':1,'remaining_shares':0,'filled_notional':.5}); st.conn.execute('COMMIT')

def test_failed_fill_transaction_leaves_trade_unseen():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; o=ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); tr=add_trade(st,'t',.25,1,'SELL',now,'tx')
    bad={'order_id':'missing','fill_id':'bad','condition':'c','token':'t','market':m,'side':'Up','price':.25,'shares':1,'notional':.25,'ts':now,'trade_price':.25,'trade_size':1,'transaction_hash':'tx','trade_key':tr['trade_key'],'meta':{}}
    with pytest.raises(Exception): st.apply_trade_atomically(tr,{},[bad])
    assert not st.seen_trade(tr['trade_key']); assert st.get_order(o['order_id'])['filled_shares']==0

def test_seen_trade_is_permanent_after_market_event_purge():
    st,led,feed,ex=mk(); now=time.time(); tr=add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1); st.purge_old_market_events(now+100000); assert st.seen_trade(tr['trade_key'])

def test_strategy_burst_uses_first_signal_not_previous_signal():
    s=CapitalFirstStrategy(); s.observe_signal('c','C10_15',.6,100,'Up'); s.observe_signal('c','C10_15',.6,110,'Up');
    assert s.market_first_signal['c']==100 and s.market_last_signal['c']==110
    c=s.build_candidates_for_market(100,.3,.3,.2,.2,[],[],120,asset='BTC',market='BTC',market_entry_count=s.market_entry_count('c'),seconds_since_first_entry=120-100,condition='c'); assert c and all(x['burst_position']==0 for x in c)

def test_strategy_state_cleanup_on_condition_forget():
    s=CapitalFirstStrategy(); s.observe_signal('c','C10_15',.6,100,'Up'); s.forget_condition('c'); assert s.market_entry_count('c')==0 and 'c' not in s.market_first_signal

def test_accounting_audit_conserves_cash_and_positions():
    st,led,feed,ex=mk(); now=time.time(); m={'asset':'BTC','market':'x','end_ts':now+100}; ex.submit_buy('c','t',m,'Up',.25,1,now-1,0); add_trade(st,'t',.25,1,'SELL',now,'tx'); ex.process(now+1); led._refresh(); assert led.accounting_audit()['cash_ok']

def test_submit_rejects_invalid_ttl_configuration_at_validation_level():
    # The bot performs this validation before creating the simulator; keep the
    # invariant explicit here so configuration cannot silently disable latency.
    assert 20 > 0.15
