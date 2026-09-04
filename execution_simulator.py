from __future__ import annotations
import hashlib, json, os, threading, time, uuid, sqlite3
from collections import defaultdict
from pathlib import Path
from state_store import StateStore

WS_URL='wss://ws-subscriptions-clob.polymarket.com/ws/market'

def num(v, default=0.0):
    try:return float(v)
    except (TypeError,ValueError):return default

def event_ts(v, default=None):
    x=num(v, default if default is not None else time.time())
    if x > 1e12: x /= 1000.0
    return x

def event_key(tr):
    tx=str(tr.get('transaction_hash') or '').strip()
    token=str(tr.get('token_id',''))
    price=num(tr.get('price')); size=num(tr.get('size')); side=str(tr.get('side','')).upper()
    # Canonicalize numeric values. A timestamp is deliberately excluded when a
    # transaction hash exists so harmless timestamp-format differences cannot
    # replay the same execution. Without a hash, timestamp remains part of the
    # best available public-event identity.
    if tx:
        raw=f"tx|{tx}|{token}|{price:.12f}|{size:.12f}|{side}"
    else:
        ts=num(tr.get('timestamp'))
        raw=f"fallback|{token}|{price:.12f}|{size:.12f}|{side}|{ts:.3f}"
    return 'trade-'+hashlib.sha256(raw.encode()).hexdigest()


class MarketFeed:
    def __init__(self, store=None):
        self._lock=threading.RLock(); self.store=store; self.books={}; self.connected=False; self.generation=0
        self.message_count=self.trade_count=self.book_count=self.price_change_count=0; self.last_trade_at=0.0; self.last_message_at=0.0
        self.last_message_by_token={}; self.last_book_by_token={}; self.last_trade_by_token={}; self._confirmed=set(); self._last_event_ts_by_token={}
        self._ws=None; self._desired=set(); self._subscribed=set(); self._stop=threading.Event(); self._thread=None
    def start(self):
        if self._thread and self._thread.is_alive():return
        import websocket
        self._stop.clear(); self._thread=threading.Thread(target=self._run,name='clob-feed',daemon=False); self._thread.start()
    def stop(self):
        self._stop.set(); ws=self._ws
        if ws:
            try:ws.close()
            except Exception:pass
        if self._thread and self._thread.is_alive():self._thread.join(timeout=8)
        self.connected=False
    def bid_depth(self, token, price, tolerance=1e-9):
        token=str(token); target=float(price)
        with self._lock:
            book=self.books.get(token) or {}; bids=book.get('bids') or {}
            for p,s in bids.items():
                if abs(float(p)-target)<=tolerance: return float(s)
        return None

    def token_fresh(self, token, now=None, max_age=15.0):
        now=time.time() if now is None else float(now); token=str(token)
        with self._lock:
            return token in self._desired and token in self._confirmed and token in self.books and now-self.last_message_by_token.get(token,0.0)<=float(max_age) and now-self.last_book_by_token.get(token,0.0)<=float(max_age)

    def set_assets(self, token_ids):
        desired={str(x) for x in token_ids if x}
        with self._lock:
            old=set(self._desired); self._desired=desired; ws=self._ws; connected=self.connected
            for token in desired-old:
                self.books.pop(token,None); self._confirmed.discard(token); self.last_book_by_token.pop(token,None); self._last_event_ts_by_token.pop(token,None)
        if ws and connected:
            # Subscription changes are best effort; reconnect loop will restore
            # the complete desired set if the socket drops during an update.
            try:
                with self._lock: current=set(getattr(self,'_subscribed',set()))
                add=sorted(desired-current); rem=sorted(current-desired)
                if rem:ws.send(json.dumps({'operation':'unsubscribe','assets_ids':rem}))
                if add:ws.send(json.dumps({'operation':'subscribe','assets_ids':add}))
                with self._lock:self._subscribed=desired
            except Exception: self._force_close()
    def _force_close(self):
        try:
            if self._ws:self._ws.close()
        except Exception:pass
    def _run(self):
        import websocket
        backoff=1.0
        while not self._stop.is_set():
            try:
                ws=websocket.create_connection(WS_URL,timeout=10,enable_multithread=True)
                ws.settimeout(1.0)
                with self._lock:
                    self._ws=ws; self.connected=True; self.generation+=1; desired=set(self._desired); self._subscribed=set(); self._confirmed=set()
                    for token in desired:
                        self.books.pop(token,None); self.last_book_by_token.pop(token,None); self._last_event_ts_by_token.pop(token,None)
                ws.send(json.dumps({'assets_ids':sorted(desired),'type':'market'}))
                # A subscription is considered confirmed only after data for a token is observed.
                with self._lock:self._subscribed=set(desired)
                last_ping=time.monotonic(); backoff=1.0
                while not self._stop.is_set():
                    if time.monotonic()-last_ping>=10:
                        ws.send('PING'); last_ping=time.monotonic()
                    try:raw=ws.recv()
                    except websocket.WebSocketTimeoutException:continue
                    if raw is None:raise ConnectionError('websocket closed')
                    self._handle(raw)
            except Exception:
                with self._lock:self.connected=False
                self._force_close()
                if not self._stop.wait(backoff):backoff=min(30.0,backoff*2)
            finally:
                with self._lock:self._ws=None; self.connected=False
    def _handle(self, raw):
        if isinstance(raw,bytes):raw=raw.decode('utf-8','replace')
        if raw in ('PONG','PING','pong','ping','') : return
        try:data=json.loads(raw)
        except Exception:return
        msgs=data if isinstance(data,list) else [data]
        for raw_msg in msgs:
            if not isinstance(raw_msg,dict):continue
            # Polymarket has emitted both flat market events and envelopes with
            # the actual event under `payload`. Accept both so a protocol-shape
            # change cannot leave the websocket connected while silently
            # starving the strategy of books/trades.
            payload=raw_msg.get('payload')
            if isinstance(payload,dict):
                m=dict(raw_msg)
                m.update(payload)
            else:
                m=raw_msg
            with self._lock:self.message_count+=1; self.last_message_at=time.time()
            aid=str(m.get('asset_id') or m.get('assetId') or m.get('token_id') or m.get('tokenId') or '')
            if aid:
                with self._lock:self.last_message_by_token[aid]=time.time()
            typ=str(m.get('event_type') or m.get('eventType') or m.get('type') or '').lower()
            if typ=='book':self._book(m)
            elif typ=='price_change':self._price_change(m)
            elif typ=='best_bid_ask':self._bbo(m)
            elif typ=='last_trade_price':self._trade(m)
            elif typ=='tick_size_change':pass
    def _book(self,m):
        token=str(m.get('asset_id') or m.get('assetId') or m.get('token_id') or m.get('tokenId') or '');
        if not token:return
        bids={}; asks={}
        for x in m.get('bids') or []:
            p=num(x.get('price') if isinstance(x,dict) else x[0]); s=num(x.get('size') if isinstance(x,dict) else x[1])
            if p>0 and s>=0:bids[p]=s
        for x in m.get('asks') or []:
            p=num(x.get('price') if isinstance(x,dict) else x[0]); s=num(x.get('size') if isinstance(x,dict) else x[1])
            if p>0 and s>=0:asks[p]=s
        with self._lock:
            ts=event_ts(m.get('timestamp'))
            prev=self._last_event_ts_by_token.get(token,0.0)
            if ts + 1e-6 < prev: return
            self._last_event_ts_by_token[token]=max(prev,ts)
            self.books[token]={'bids':bids,'asks':asks,'ts':ts,'hash':m.get('hash'),'snapshot':True}; self.book_count+=1; self.last_book_by_token[token]=time.time(); self._confirmed.add(token)
    def _price_change(self,m):
        ts=event_ts(m.get('timestamp'))
        with self._lock:self.price_change_count+=1
        for x in m.get('price_changes') or []:
            token=str(x.get('asset_id') or x.get('assetId') or x.get('token_id') or x.get('tokenId') or ''); side=str(x.get('side','')).upper(); p=num(x.get('price')); s=num(x.get('size'))
            if not token or p<=0 or side not in ('BUY','SELL'):continue
            with self._lock:
                prev=self._last_event_ts_by_token.get(token,0.0)
                if ts + 1e-6 < prev: continue
                self._last_event_ts_by_token[token]=max(prev,ts)
                self.last_message_by_token[token]=time.time()
                b=self.books.get(token)
                if not b or not b.get('snapshot'): continue
                levels=b['bids'] if side=='BUY' else b['asks']
                if s<=0:levels.pop(p,None)
                else:levels[p]=s
                b['ts']=ts; self.last_book_by_token[token]=time.time()
    def _bbo(self,m):
        token=str(m.get('asset_id') or m.get('assetId') or m.get('token_id') or m.get('tokenId') or '');
        if not token:return
        with self._lock:
            ts=event_ts(m.get('timestamp')); prev=self._last_event_ts_by_token.get(token,0.0)
            if ts + 1e-6 < prev:return
            self._last_event_ts_by_token[token]=max(prev,ts)
            if token not in self.books or not self.books[token].get('snapshot'): return
            self.books[token]=dict(self.books[token],best_bid=num(m.get('best_bid',m.get('bestBid')),0),best_ask=num(m.get('best_ask',m.get('bestAsk')),0),ts=ts); self.last_book_by_token[token]=time.time()
    def _trade(self,m):
        token=str(m.get('asset_id') or m.get('assetId') or m.get('token_id') or m.get('tokenId') or ''); p=num(m.get('price')); s=num(m.get('size')); side=str(m.get('side','')).upper(); ts=event_ts(m.get('timestamp'),0.0)
        if not token or not (p>0 and s>0) or side not in ('BUY','SELL') or ts<=0:return
        if ts>1e12:ts/=1000
        tr={'token_id':token,'price':p,'size':s,'side':side,'timestamp':ts,'transaction_hash':m.get('transaction_hash') or m.get('transactionHash')}
        tr['trade_key']=event_key(tr)
        with self._lock:
            if self.store is not None:
                with self.store.tx():
                    self.store.conn.execute('BEGIN IMMEDIATE')
                    before=self.store.conn.total_changes
                    self.store.add_market_trade(tr)
                    inserted=self.store.conn.total_changes>before
                    self.store.conn.execute('COMMIT')
                if not inserted:return
            self.trade_count+=1; self.last_trade_at=max(self.last_trade_at,ts); self.last_trade_by_token[token]=time.time()

class RestingOrderSimulator:
    def __init__(self, feed, store_or_path, latency_ms=150, ttl=20, queue_safety=1.0, min_fill_shares=0.0001):
        self.feed=feed; self.store=store_or_path if isinstance(store_or_path,StateStore) else StateStore(store_or_path)
        self.latency_ms=float(latency_ms); self.ttl=float(ttl); self.queue_safety=max(0,float(queue_safety)); self.min_fill_shares=max(0,float(min_fill_shares)); self._lock=threading.RLock()
        self.orders={o['order_id']:o for o in self.store.get_orders()}; self.fills=self.store.get_fills(); self._queue_order_index=defaultdict(list)
        for o in self.orders.values():
            if o['status']=='RESTING':self._queue_order_index[self.qkey(o['token'],o['price'])].append(o['order_id'])
    @staticmethod
    def qkey(token,price):return f'{token}|{float(price):.9f}'
    def active_orders(self):
        with self._lock:return [dict(o) for o in self.orders.values() if o['status']=='RESTING']
    def submit_buy(self,condition,token,market,side,price,shares,ts,queue_hint=0,meta=None):
        price=float(price); shares=float(shares); ts=float(ts)
        if side not in ('Up','Down') or not (0<price<1) or shares<=0:raise ValueError('invalid simulated order')
        with self._lock:
            qk=self.qkey(token,price); ids=self._queue_order_index[qk]; own_ahead=sum(max(0,float(self.orders[i]['remaining_shares'])) for i in ids if self.orders[i]['status']=='RESTING')
            oid='sim-'+uuid.uuid4().hex
            meta=dict(meta or {}); meta.setdefault('market',market); meta.setdefault('end_ts',market.get('end_ts') if isinstance(market,dict) else 0); meta.setdefault('asset',market.get('asset','') if isinstance(market,dict) else '')
            o={'order_id':oid,'condition':str(condition),'token':str(token),'side':side,'price':price,'requested_shares':shares,'remaining_shares':shares,'filled_shares':0.0,'filled_notional':0.0,'submitted_ts':ts,'active_ts':ts+self.latency_ms/1000,'expires_ts':ts+self.ttl,'queue_ahead':max(0,float(queue_hint))*self.queue_safety+own_ahead,'status':'RESTING','feed_generation':self.feed.generation,'meta':meta}
            with self.store.tx():
                self.store.conn.execute('BEGIN IMMEDIATE'); self.store.put_order(o); self.store.append_event('order:'+oid,'ORDER_ACCEPTED',ts,o); self.store.conn.execute('COMMIT')
            self.orders[oid]=o; self._queue_order_index[qk].append(oid); return dict(o)
    def cancel(self,oid,reason='CANCELLED',ts=None):
        ts=time.time() if ts is None else float(ts)
        with self._lock:
            o=self.orders.get(oid)
            if not o or o['status']!='RESTING':return None
            updated=dict(o); updated['status']=reason; updated['cancelled_ts']=ts
            with self.store.tx():
                self.store.conn.execute('BEGIN IMMEDIATE'); self.store.put_order(updated); self.store.append_event(f'order:{oid}:{reason}:{int(ts*1000)}',reason,ts,updated); self.store.conn.execute('COMMIT')
            self.orders[oid]=updated
            return dict(updated)
    def token_fresh(self, token, now=None, max_age=15.0):
        return self.feed.token_fresh(token, now, max_age)

    def process(self,now=None):
        now=time.time() if now is None else float(now); result=[]
        trades=self.store.get_unseen_market_trades(now)
        for tr in trades:
            if str(tr.get('side','')).upper()!='SELL':
                try:self.store.apply_trade_atomically(tr,{},[])
                except RuntimeError: continue
                continue
            key=str(tr['trade_key']); ts=float(tr['timestamp']); remaining=float(tr['size']); token=str(tr['token_id']); tp=float(tr['price'])
            with self._lock:
                eligible=[]
                for o in self.orders.values():
                    if o['status']!='RESTING' or o['token']!=token: continue
                    end_ts=float((o.get('meta') or {}).get('end_ts') or 0.0)
                    if ts < float(o['active_ts']) or ts >= float(o['expires_ts']) or (end_ts and ts>=end_ts): continue
                    # A public execution at a different price does not prove that
                    # our simulated passive order executed. Use exact-price prints.
                    if abs(tp-float(o['price']))>1e-9: continue
                    eligible.append(o)
                eligible.sort(key=lambda o:(-float(o['price']),float(o['submitted_ts']),o['order_id']))
                queue_updates={}; fills=[]; available_cash=float(self.store.get_meta('cash',0.0))
                for o in eligible:
                    if remaining<=1e-12: break
                    ahead=max(0.0,float(queue_updates.get(o['order_id'],o.get('queue_ahead',0.0)))); consumed=min(ahead,remaining)
                    new_ahead=ahead-consumed; remaining-=consumed; queue_updates[o['order_id']]=new_ahead
                    if remaining<=1e-12: continue
                    qty=min(float(o['remaining_shares']),remaining,available_cash/max(float(o['price']),1e-12))
                    if qty < self.min_fill_shares: continue
                    fid='fill-'+hashlib.sha256(f'{o["order_id"]}|{key}'.encode()).hexdigest()[:40]
                    fills.append({'order_id':o['order_id'],'fill_id':fid,'condition':o['condition'],'token':o['token'],'market':o.get('meta',{}).get('market',{}),'side':o['side'],'price':float(o['price']),'shares':qty,'notional':qty*float(o['price']),'ts':ts,'trade_price':tp,'trade_size':float(tr['size']),'transaction_hash':tr.get('transaction_hash'),'trade_key':key,'execution':'CLOB_PUBLIC_TRADE_MATCH','meta':dict(o.get('meta',{}))})
                    # Earlier simulated orders at the same price are ahead of
                    # later ones. A fill removes that own-queue volume for later
                    # orders before the next public print.
                    for later in eligible:
                        if later is o: continue
                        if float(later['price']) == float(o['price']) and float(later['submitted_ts']) > float(o['submitted_ts']):
                            queue_updates[later['order_id']]=max(0.0, float(queue_updates.get(later['order_id'], later.get('queue_ahead',0.0))) - qty)
                    remaining-=qty; available_cash-=qty*float(o['price'])
                try: committed=self.store.apply_trade_atomically(tr,queue_updates,fills)
                except (RuntimeError, sqlite3.Error):
                    # Do not acknowledge a trade if any mutation failed. The event remains
                    # durable and unseen, so the next pass can retry it.
                    continue
                if not committed: continue
                for oid in queue_updates:
                    fresh=self.store.get_order(oid)
                    if fresh is not None: self.orders[oid]=fresh
                for f in fills:
                    fresh=self.store.get_order(f['order_id']); self.orders[f['order_id']]=fresh; self.fills.append(f); result.append(f)
        for o in list(self.orders.values()):
            if o['status']=='RESTING' and now>=float(o['expires_ts']): self.cancel(o['order_id'],'EXPIRED',now)
        return result

    def cancel_all_for_condition(self,condition,reason='MARKET_END',ts=None):
        for o in list(self.orders.values()):
            if o['status']=='RESTING' and o['condition']==condition:self.cancel(o['order_id'],reason,ts)
    def save(self):
        with self.store.tx():self.store.conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
