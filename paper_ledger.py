from __future__ import annotations
import json, time
from pathlib import Path
from state_store import StateStore

class PaperLedger:
    def __init__(self, store_or_path, initial_cash=1000):
        self.store = store_or_path if isinstance(store_or_path, StateStore) else StateStore(store_or_path)
        self.initial_cash=float(initial_cash)
        with self.store.tx():
            if self.store.get_meta('initial_cash') is None:
                self.store.set_meta('initial_cash', self.initial_cash)
                self.store.set_meta('cash', self.initial_cash)
                self.store.set_meta('peak_equity', self.initial_cash)
        self._refresh()
    def _refresh(self):
        self.cash=float(self.store.get_meta('cash', self.initial_cash)); self.positions={}
        for p in self.store.get_positions():
            p=dict(p); p['condition']=p.get('condition_id'); p['token']=p.get('token'); p['avg']=p.get('avg_price'); p['market']=p.get('market'); self.positions[p['position_key']]=p
        self.trades=self.store.get_ledger_trades(); self.realized=sum(float(t.get('pnl',0)) for t in self.trades if t.get('action')=='SETTLE')
    def save(self): self._refresh()
    def total_open_cost(self): return sum(float(p['cost']) for p in self.positions.values())
    def exposure(self,condition): return sum(float(p['cost']) for p in self.positions.values() if p['condition_id']==condition)
    def buy_fill(self, *args, **kwargs):
        """Legacy API intentionally disabled: fills must come from the CLOB simulator.

        Keeping a second direct-fill mutation path would bypass the durable public
        trade event and execution invariants.
        """
        raise RuntimeError('direct paper fills are disabled; use RestingOrderSimulator')

    def settle(self, condition, winner_token, ts=None):
        ts=time.time() if ts is None else float(ts); closed=[]
        with self.store.tx():
            self.store.conn.execute('BEGIN IMMEDIATE')
            try:
                rows=self.store.conn.execute('SELECT * FROM positions WHERE condition_id=?',(condition,)).fetchall()
                cash=float(self.store.get_meta('cash',self.initial_cash))
                for r in rows:
                    p=dict(r); payout=p['shares'] if p['token']==winner_token else 0.0; pnl=payout-p['cost']; sid=f'settle:{condition}:{p["token"]}'
                    payload={'ts':ts,'action':'SETTLE','condition':condition,'token':p['token'],'side':p['side'],'price':p['avg_price'],'shares':p['shares'],'notional':p['cost'],'payout':payout,'pnl':pnl,'settlement_per_share':1.0 if payout else 0.0,'status':'WIN' if pnl>=0 else 'LOSS'}
                    existing=self.store.conn.execute('SELECT 1 FROM ledger_trades WHERE event_id=?',(sid,)).fetchone()
                    if existing:
                        self.store.conn.execute('DELETE FROM positions WHERE position_key=?',(p['position_key'],)); continue
                    self.store.conn.execute('INSERT INTO ledger_trades(event_id,ts,action,condition_id,token,side,price,shares,notional,payout,pnl,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(sid,ts,'SETTLE',condition,p['token'],p['side'],p['avg_price'],p['shares'],p['cost'],payout,pnl,json.dumps(payload,separators=(',',':'),sort_keys=True)))
                    cash+=payout; closed.append(payload); self.store.conn.execute('DELETE FROM positions WHERE position_key=?',(p['position_key'],)); self.store.append_event(sid,'SETTLE',ts,payload)
                self.store.set_meta('cash',cash); self.store.conn.execute('COMMIT')
            except Exception:
                self.store.conn.execute('ROLLBACK'); raise
        self._refresh(); return closed
    def accounting_audit(self):
        initial=float(self.store.get_meta('initial_cash',self.initial_cash)); cash=float(self.store.get_meta('cash',self.initial_cash))
        buys=sum(float(t.get('notional',0) or 0) for t in self.trades if t.get('action')=='BUY')
        payouts=sum(float(t.get('payout',0) or 0) for t in self.trades if t.get('action')=='SETTLE')
        position_cost=self.total_open_cost()
        expected_cash=initial-buys+payouts
        if abs(cash-expected_cash)>1e-6: raise RuntimeError(f'cash conservation failed: cash={cash} expected={expected_cash}')
        buy_by_pos={}
        for t in self.trades:
            if t.get('action')=='BUY': buy_by_pos[(t.get('condition'),t.get('token'))]=buy_by_pos.get((t.get('condition'),t.get('token')),0.0)+float(t.get('notional',0) or 0)
        settled_by_pos={}
        for t in self.trades:
            if t.get('action')=='SETTLE': settled_by_pos[(t.get('condition'),t.get('token'))]=settled_by_pos.get((t.get('condition'),t.get('token')),0.0)+float(t.get('notional',0) or 0)
        expected_open=sum(max(0.0,buy_by_pos[k]-settled_by_pos.get(k,0.0)) for k in buy_by_pos)
        if abs(position_cost-expected_open)>1e-6: raise RuntimeError(f'position conservation failed: open={position_cost} expected={expected_open}')
        return {'cash_ok':True,'position_ok':True}

    def mark(self, books):
        self._refresh(); value=0; marked=0
        for p in self.positions.values():
            px=books.get(p['token'],p['avg_price']); value+=p['shares']*px; marked += 1 if p['token'] in books else 0
        equity=self.cash+value; peak=max(float(self.store.get_meta('peak_equity',self.initial_cash)),equity)
        with self.store.tx():
            self.store.set_meta('peak_equity',equity if equity>peak else peak)
        return {'cash':self.cash,'open_cost':self.total_open_cost(),'market_value':value,'unrealized':value-self.total_open_cost(),'realized':self.realized,'equity':equity,'pnl':equity-self.initial_cash,'drawdown':equity-peak,'marked':marked}
