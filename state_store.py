from __future__ import annotations
import json, sqlite3, threading
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,event_type TEXT NOT NULL,ts REAL NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders (
 order_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, token TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('Up','Down')), price REAL NOT NULL CHECK(price>0 AND price<1),
 requested_shares REAL NOT NULL CHECK(requested_shares>0), remaining_shares REAL NOT NULL CHECK(remaining_shares>=-1e-9),
 filled_shares REAL NOT NULL CHECK(filled_shares>=-1e-9), filled_notional REAL NOT NULL CHECK(filled_notional>=-1e-9),
 submitted_ts REAL NOT NULL, active_ts REAL NOT NULL, expires_ts REAL NOT NULL,
 queue_ahead REAL NOT NULL CHECK(queue_ahead>=-1e-9), status TEXT NOT NULL CHECK(status IN ('RESTING','FILLED','EXPIRED','MARKET_END','CANCELLED')),
 created_generation INTEGER NOT NULL, meta TEXT NOT NULL,
 CHECK(filled_shares + remaining_shares <= requested_shares + 1e-7),
 CHECK(filled_notional <= price*requested_shares + 1e-7),
 CHECK((status='RESTING' AND remaining_shares>1e-12) OR (status='FILLED' AND remaining_shares<=1e-12) OR status IN ('EXPIRED','MARKET_END','CANCELLED'))
);
CREATE TABLE IF NOT EXISTS fills (
 fill_id TEXT PRIMARY KEY, order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT, trade_key TEXT NOT NULL, ts REAL NOT NULL,
 price REAL NOT NULL CHECK(price>0 AND price<1), shares REAL NOT NULL CHECK(shares>0), notional REAL NOT NULL CHECK(notional>0),
 trade_price REAL NOT NULL CHECK(trade_price>0 AND trade_price<1), trade_size REAL NOT NULL CHECK(trade_size>0), transaction_hash TEXT, payload TEXT NOT NULL,
 UNIQUE(order_id,trade_key)
);
CREATE TABLE IF NOT EXISTS seen_trades (trade_key TEXT PRIMARY KEY, ts REAL NOT NULL);
CREATE TABLE IF NOT EXISTS market_trades (trade_key TEXT PRIMARY KEY,ts REAL NOT NULL,token TEXT NOT NULL,price REAL NOT NULL CHECK(price>0 AND price<1),size REAL NOT NULL CHECK(size>0),side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),transaction_hash TEXT,payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_market_trades_ts ON market_trades(ts);
CREATE TABLE IF NOT EXISTS positions (
 position_key TEXT PRIMARY KEY,condition_id TEXT NOT NULL,token TEXT NOT NULL,side TEXT NOT NULL CHECK(side IN ('Up','Down')),market TEXT NOT NULL,
 shares REAL NOT NULL CHECK(shares>=-1e-9),cost REAL NOT NULL CHECK(cost>=-1e-9),avg_price REAL NOT NULL CHECK(avg_price>0 AND avg_price<1),asset TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_trades (
 row_id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,ts REAL NOT NULL,action TEXT NOT NULL CHECK(action IN ('BUY','SETTLE')),
 condition_id TEXT,token TEXT,side TEXT,price REAL,shares REAL,notional REAL,payout REAL,pnl REAL,payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_condition_status ON orders(condition_id,status);
CREATE INDEX IF NOT EXISTS idx_fills_trade_key ON fills(trade_key);
CREATE INDEX IF NOT EXISTS idx_ledger_action_ts ON ledger_trades(action,ts);
"""

class StateStore:
    def __init__(self,path):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True); self._lock=threading.RLock()
        self.conn=sqlite3.connect(self.path,timeout=30,isolation_level=None,check_same_thread=False); self.conn.row_factory=sqlite3.Row
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._migrate_tables_if_needed()
            self.conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS trg_order_bounds_insert BEFORE INSERT ON orders BEGIN
              SELECT CASE WHEN NEW.filled_shares < -1e-9 OR NEW.remaining_shares < -1e-9 OR NEW.filled_shares+NEW.remaining_shares > NEW.requested_shares+1e-7 THEN RAISE(ABORT,'order share invariant violated') END;
              SELECT CASE WHEN NEW.filled_notional < -1e-9 OR NEW.filled_notional > NEW.price*NEW.requested_shares+1e-7 THEN RAISE(ABORT,'order notional invariant violated') END;
              SELECT CASE WHEN NEW.status='RESTING' AND NEW.remaining_shares<=1e-12 THEN RAISE(ABORT,'resting order has no remaining shares') END;
              SELECT CASE WHEN NEW.status='FILLED' AND NEW.remaining_shares>1e-12 THEN RAISE(ABORT,'filled order has remaining shares') END;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_order_bounds BEFORE UPDATE OF price,requested_shares,filled_shares,filled_notional,remaining_shares,status ON orders BEGIN
              SELECT CASE WHEN NEW.filled_shares < -1e-9 OR NEW.remaining_shares < -1e-9 OR NEW.filled_shares+NEW.remaining_shares > NEW.requested_shares+1e-7 THEN RAISE(ABORT,'order share invariant violated') END;
              SELECT CASE WHEN NEW.filled_notional < -1e-9 OR NEW.filled_notional > NEW.price*NEW.requested_shares+1e-7 THEN RAISE(ABORT,'order notional invariant violated') END;
              SELECT CASE WHEN NEW.status='RESTING' AND NEW.remaining_shares<=1e-12 THEN RAISE(ABORT,'resting order has no remaining shares') END;
              SELECT CASE WHEN NEW.status='FILLED' AND NEW.remaining_shares>1e-12 THEN RAISE(ABORT,'filled order has remaining shares') END;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_fill_bounds BEFORE INSERT ON fills BEGIN
              SELECT CASE WHEN NEW.shares<=0 OR NEW.notional<=0 OR NEW.shares*NEW.price < NEW.notional-1e-7 OR NEW.shares*NEW.price > NEW.notional+1e-7 THEN RAISE(ABORT,'fill notional invariant violated') END;
            END;
            """)
            self.conn.execute('PRAGMA foreign_keys=ON'); self.conn.execute('PRAGMA busy_timeout=30000')
            self._validate_schema()

    def _migrate_tables_if_needed(self):
        """Upgrade older V21 tables while preserving valid financial history."""
        def has_fill_fk():
            return any(r['table']=='orders' and r['from']=='order_id' for r in self.conn.execute('PRAGMA foreign_key_list(fills)'))
        if has_fill_fk():
            self.conn.executescript('DROP TRIGGER IF EXISTS trg_order_bounds_insert; DROP TRIGGER IF EXISTS trg_order_bounds; DROP TRIGGER IF EXISTS trg_fill_bounds;')
            return
        self.conn.execute('PRAGMA foreign_keys=OFF')
        self.conn.execute('BEGIN IMMEDIATE')
        try:
            self.conn.execute('ALTER TABLE orders RENAME TO orders_old_v21')
            self.conn.execute("""CREATE TABLE orders (
 order_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, token TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('Up','Down')), price REAL NOT NULL CHECK(price>0 AND price<1),
 requested_shares REAL NOT NULL CHECK(requested_shares>0), remaining_shares REAL NOT NULL CHECK(remaining_shares>=-1e-9),
 filled_shares REAL NOT NULL CHECK(filled_shares>=-1e-9), filled_notional REAL NOT NULL CHECK(filled_notional>=-1e-9),
 submitted_ts REAL NOT NULL, active_ts REAL NOT NULL, expires_ts REAL NOT NULL, queue_ahead REAL NOT NULL CHECK(queue_ahead>=-1e-9),
 status TEXT NOT NULL CHECK(status IN ('RESTING','FILLED','EXPIRED','MARKET_END','CANCELLED')), created_generation INTEGER NOT NULL, meta TEXT NOT NULL,
 CHECK(filled_shares+remaining_shares<=requested_shares+1e-7), CHECK(filled_notional<=price*requested_shares+1e-7),
 CHECK((status='RESTING' AND remaining_shares>1e-12) OR (status='FILLED' AND remaining_shares<=1e-12) OR status IN ('EXPIRED','MARKET_END','CANCELLED'))
)""")
            self.conn.execute("""INSERT INTO orders SELECT order_id,condition_id,token,side,price,requested_shares,remaining_shares,filled_shares,filled_notional,submitted_ts,active_ts,expires_ts,queue_ahead,status,created_generation,meta FROM orders_old_v21""")
            self.conn.execute('DROP TABLE orders_old_v21')
            self.conn.execute('ALTER TABLE fills RENAME TO fills_old_v21')
            self.conn.execute("""CREATE TABLE fills (
 fill_id TEXT PRIMARY KEY, order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT, trade_key TEXT NOT NULL, ts REAL NOT NULL,
 price REAL NOT NULL CHECK(price>0 AND price<1), shares REAL NOT NULL CHECK(shares>0), notional REAL NOT NULL CHECK(notional>0),
 trade_price REAL NOT NULL CHECK(trade_price>0 AND trade_price<1), trade_size REAL NOT NULL CHECK(trade_size>0), transaction_hash TEXT, payload TEXT NOT NULL,
 UNIQUE(order_id,trade_key)
)""")
            orphan=self.conn.execute('SELECT count(*) FROM fills_old_v21 f LEFT JOIN orders o ON o.order_id=f.order_id WHERE o.order_id IS NULL').fetchone()[0]
            if orphan: raise RuntimeError(f'cannot migrate {orphan} orphan fill records')
            self.conn.execute('INSERT INTO fills SELECT f.fill_id,f.order_id,f.trade_key,f.ts,f.price,f.shares,f.notional,f.trade_price,f.trade_size,f.transaction_hash,f.payload FROM fills_old_v21 f')
            self.conn.execute('DROP TABLE fills_old_v21')
            self.conn.execute('COMMIT')
        except Exception:
            self.conn.execute('ROLLBACK'); raise
        finally:
            self.conn.execute('PRAGMA foreign_keys=ON')
        self.conn.executescript('DROP TRIGGER IF EXISTS trg_order_bounds_insert; DROP TRIGGER IF EXISTS trg_order_bounds; DROP TRIGGER IF EXISTS trg_fill_bounds; CREATE INDEX IF NOT EXISTS idx_orders_condition_status ON orders(condition_id,status); CREATE INDEX IF NOT EXISTS idx_fills_trade_key ON fills(trade_key); CREATE INDEX IF NOT EXISTS idx_ledger_action_ts ON ledger_trades(action,ts); CREATE INDEX IF NOT EXISTS idx_market_trades_ts ON market_trades(ts);')

    def _validate_schema(self):
        required={'orders','fills','market_trades','seen_trades','positions','ledger_trades','events','meta'}
        actual={r['name'] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing=required-actual
        if missing: raise RuntimeError(f'database schema incomplete: {sorted(missing)}')
        fk=list(self.conn.execute('PRAGMA foreign_key_list(fills)'))
        if not any(r['table']=='orders' and r['from']=='order_id' for r in fk):
            raise RuntimeError('database was created by an incompatible V21 build; start with FRESH_START=true or migrate explicitly')

    def close(self):
        with self._lock:self.conn.close()
    def tx(self): return self._lock
    def event_exists(self,e): return self.conn.execute('SELECT 1 FROM events WHERE event_id=?',(e,)).fetchone() is not None
    def append_event(self,e,t,ts,p): self.conn.execute('INSERT OR IGNORE INTO events(event_id,event_type,ts,payload) VALUES(?,?,?,?)',(e,t,float(ts),json.dumps(p,separators=(',',':'),sort_keys=True)))
    def _order(self,r):
        d=dict(r); d['condition']=d['condition_id']; d['meta']=json.loads(d.pop('meta') or '{}'); return d
    def get_orders(self): return [self._order(r) for r in self.conn.execute('SELECT * FROM orders ORDER BY submitted_ts,order_id')]
    def get_order(self,oid):
        r=self.conn.execute('SELECT * FROM orders WHERE order_id=?',(oid,)).fetchone(); return self._order(r) if r else None
    def put_order(self,o):
        self.conn.execute('''INSERT INTO orders(order_id,condition_id,token,side,price,requested_shares,remaining_shares,filled_shares,filled_notional,submitted_ts,active_ts,expires_ts,queue_ahead,status,created_generation,meta) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(order_id) DO UPDATE SET condition_id=excluded.condition_id,token=excluded.token,side=excluded.side,price=excluded.price,requested_shares=excluded.requested_shares,remaining_shares=excluded.remaining_shares,filled_shares=excluded.filled_shares,filled_notional=excluded.filled_notional,submitted_ts=excluded.submitted_ts,active_ts=excluded.active_ts,expires_ts=excluded.expires_ts,queue_ahead=excluded.queue_ahead,status=excluded.status,created_generation=excluded.created_generation,meta=excluded.meta''',
        (o['order_id'],o['condition'],o['token'],o['side'],float(o['price']),float(o['requested_shares']),float(o['remaining_shares']),float(o['filled_shares']),float(o['filled_notional']),float(o['submitted_ts']),float(o['active_ts']),float(o['expires_ts']),float(o['queue_ahead']),o['status'],int(o.get('feed_generation',o.get('created_generation',0))),json.dumps(o.get('meta') or {},separators=(',',':'),sort_keys=True)))
    def get_fills(self):
        out=[]
        for r in self.conn.execute('SELECT * FROM fills ORDER BY rowid'):
            d=dict(r); d.update(json.loads(d.pop('payload') or '{}')); out.append(d)
        return out
    def has_fill(self,fid): return self.conn.execute('SELECT 1 FROM fills WHERE fill_id=?',(fid,)).fetchone() is not None
    def add_fill(self,f):
        self.conn.execute('INSERT INTO fills(fill_id,order_id,trade_key,ts,price,shares,notional,trade_price,trade_size,transaction_hash,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(f['fill_id'],f['order_id'],f['trade_key'],float(f['ts']),float(f['price']),float(f['shares']),float(f['notional']),float(f['trade_price']),float(f['trade_size']),f.get('transaction_hash'),json.dumps(f,separators=(',',':'),sort_keys=True)))
    def add_market_trade(self,t):
        self.conn.execute('INSERT OR IGNORE INTO market_trades(trade_key,ts,token,price,size,side,transaction_hash,payload) VALUES(?,?,?,?,?,?,?,?)',(t['trade_key'],float(t['timestamp']),str(t['token_id']),float(t['price']),float(t['size']),str(t['side']),t.get('transaction_hash'),json.dumps(t,separators=(',',':'),sort_keys=True)))
    def get_unseen_market_trades(self,now,limit=10000):
        rows=self.conn.execute('SELECT mt.payload FROM market_trades mt LEFT JOIN seen_trades st ON st.trade_key=mt.trade_key WHERE st.trade_key IS NULL AND mt.ts<=? ORDER BY mt.ts,mt.trade_key LIMIT ?',(float(now),int(limit))).fetchall(); return [json.loads(r['payload']) for r in rows]
    def seen_trade(self,k): return self.conn.execute('SELECT 1 FROM seen_trades WHERE trade_key=?',(k,)).fetchone() is not None
    def mark_trade_seen(self,k,ts): self.conn.execute('INSERT OR IGNORE INTO seen_trades(trade_key,ts) VALUES(?,?)',(k,float(ts)))
    def purge_old_market_events(self,cutoff):
        self.conn.execute('DELETE FROM market_trades WHERE ts<? AND trade_key IN (SELECT trade_key FROM seen_trades)',(float(cutoff),))
    def apply_trade_atomically(self,trade,queue_updates,fills):
        key=str(trade['trade_key']); ts=float(trade['timestamp'])
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                if self.seen_trade(key): self.conn.execute('ROLLBACK'); return False
                # Validate all requested mutations before writing any of them.
                total_notional=0.0
                for oid,q in queue_updates.items():
                    o=self.get_order(oid)
                    if o and o['status']=='RESTING':
                        if float(q)<-1e-9: raise RuntimeError('negative queue update')
                        o['queue_ahead']=float(q); self.put_order(o)
                cash=float(self.get_meta('cash',0.0))
                for f in fills:
                    if self.has_fill(f['fill_id']): continue
                    o=self.get_order(f['order_id'])
                    if not o or o['status']!='RESTING': raise RuntimeError('order unavailable during fill commit')
                    qty=float(f['shares']); notional=float(f['notional'])
                    if qty<=0 or qty>float(o['remaining_shares'])+1e-9: raise RuntimeError('fill exceeds remaining order')
                    if abs(notional-qty*float(o['price']))>1e-7: raise RuntimeError('fill notional/price mismatch')
                    total_notional += notional
                    if total_notional>cash+1e-9: raise RuntimeError('paper cash exhausted before fill commit')
                    o['remaining_shares']=max(0.0,o['remaining_shares']-qty); o['filled_shares']+=qty; o['filled_notional']+=notional; o['queue_ahead']=max(0.0,o['queue_ahead']-qty)
                    if o['remaining_shares']<=1e-12: o['remaining_shares']=0.0; o['status']='FILLED'; o['filled_ts']=float(f['ts'])
                    self.put_order(o); self.add_fill(f)
                    poskey=f"{o['condition']}:{o['token']}"; row=self.conn.execute('SELECT * FROM positions WHERE position_key=?',(poskey,)).fetchone(); meta=o.get('meta') or {}; asset=str(meta.get('asset','')); market=meta.get('market',{}); market_name=str(market.get('market',market) if isinstance(market,dict) else market)
                    if row:
                        old=dict(row); sh=old['shares']+qty; cost=old['cost']+notional; self.conn.execute('UPDATE positions SET shares=?,cost=?,avg_price=?,market=?,asset=? WHERE position_key=?',(sh,cost,cost/sh,market_name or old['market'],asset or old['asset'],poskey))
                    else:self.conn.execute('INSERT INTO positions(position_key,condition_id,token,side,market,shares,cost,avg_price,asset) VALUES(?,?,?,?,?,?,?,?,?)',(poskey,o['condition'],o['token'],o['side'],market_name,qty,notional,o['price'],asset))
                    payload={'ts':float(f['ts']),'action':'BUY','condition':o['condition'],'token':o['token'],'side':o['side'],'price':o['price'],'shares':qty,'notional':notional,**dict(f.get('meta') or {}),'sim_fill_id':f['fill_id'],'sim_order_id':o['order_id'],'trade_key':key,'trade_price':f['trade_price'],'trade_size':f['trade_size'],'transaction_hash':f.get('transaction_hash')}
                    self.conn.execute('INSERT INTO ledger_trades(event_id,ts,action,condition_id,token,side,price,shares,notional,payout,pnl,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(f'fill:{f["fill_id"]}',f['ts'],'BUY',o['condition'],o['token'],o['side'],f['price'],qty,notional,0,0,json.dumps(payload,separators=(',',':'),sort_keys=True)))
                    self.append_event(f'fill:{f["fill_id"]}','FILL',f['ts'],payload)
                self.set_meta('cash',cash-total_notional)
                self.mark_trade_seen(key,ts); self.append_event(f'trade:{key}','MARKET_TRADE_PROCESSED',ts,trade)
                self.conn.execute('COMMIT'); return True
            except Exception:
                self.conn.execute('ROLLBACK'); raise
    def get_positions(self): return [dict(r) for r in self.conn.execute('SELECT * FROM positions')]
    def get_ledger_trades(self):
        out=[]
        for r in self.conn.execute('SELECT * FROM ledger_trades ORDER BY row_id'):
            d=dict(r); d.update(json.loads(d.pop('payload') or '{}')); out.append(d)
        return out
    def get_meta(self,k,default=None):
        r=self.conn.execute('SELECT value FROM meta WHERE key=?',(k,)).fetchone(); return default if not r else r['value']
    def set_meta(self,k,v): self.conn.execute('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(k,str(v)))
    def integrity_check(self): return self.conn.execute('PRAGMA integrity_check').fetchone()[0]
