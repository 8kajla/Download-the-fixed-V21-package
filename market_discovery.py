from __future__ import annotations
import json, logging, os, re, time
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GAMMA='https://gamma-api.polymarket.com'; CLOB='https://clob.polymarket.com'
ASSETS={'BTC':'btc','ETH':'eth','SOL':'sol','BNB':'bnb','DOGE':'doge','HYPE':'hype'}
LOG=logging.getLogger('market')
SESSION=requests.Session(); SESSION.mount('https://',HTTPAdapter(max_retries=Retry(total=3,connect=3,read=3,backoff_factor=.4,status_forcelist=[429,500,502,503,504],allowed_methods=frozenset(['GET']))))
SESSION.headers.update({'User-Agent':'polymarket-paper-v21/1.0'})

def parse_bool(v, default=None):
    if isinstance(v,bool): return v
    if isinstance(v,(int,float)) and v in (0,1): return bool(v)
    if isinstance(v,str):
        x=v.strip().lower()
        if x in ('true','1','yes','on'): return True
        if x in ('false','0','no','off'): return False
    return default

def parse_list(v):
    if isinstance(v,list):return v
    if isinstance(v,str):
        try:x=json.loads(v); return x if isinstance(x,list) else []
        except json.JSONDecodeError:return []
    return []

def rows(r):
    d=r.json()
    if isinstance(d,list):return d
    if isinstance(d,dict):
        for k in ('data','markets','results'):
            if isinstance(d.get(k),list):return d[k]
    return []

def normalize(m):
    slug=str(m.get('slug') or '').strip(); mm=re.fullmatch(r'(btc|eth|sol|bnb|doge|hype|hyperliquid)-updown-5m-(\d{9,12})',slug.lower())
    if not mm:return None
    asset={'hyperliquid':'HYPE','hype':'HYPE'}[mm.group(1)] if mm.group(1) in ('hyperliquid','hype') else mm.group(1).upper()
    tokens=parse_list(m.get('clobTokenIds') or m.get('clob_token_ids')); outcomes=parse_list(m.get('outcomes'))
    if len(tokens)<2:return None
    mapping={str(o).strip().lower():str(t) for o,t in zip(outcomes,tokens)}
    up,down=mapping.get('up'),mapping.get('down')
    if not up or not down:up,down=str(tokens[0]),str(tokens[1])
    condition=str(m.get('conditionId') or m.get('condition_id') or '').strip()
    if not condition:return None
    try:start=float(mm.group(2))
    except ValueError:return None
    for key in ('startTs','start_ts'):
        try:
            if m.get(key) is not None: start=float(m[key]); break
        except (TypeError,ValueError): pass
    else:
        raw_start=m.get('startDate') or m.get('start_date')
        if raw_start:
            try:
                start=datetime.fromisoformat(str(raw_start).replace('Z','+00:00')).astimezone(timezone.utc).timestamp()
            except ValueError: pass
    end=None
    for key in ('endTs','end_ts'):
        try:
            if m.get(key) is not None: end=float(m[key]); break
        except (TypeError,ValueError): pass
    if end is None:
        raw_end=m.get('endDate') or m.get('end_date')
        if raw_end:
            try: end=datetime.fromisoformat(str(raw_end).replace('Z','+00:00')).astimezone(timezone.utc).timestamp()
            except ValueError: pass
    if end is None: end=start+300.0
    if start>1e12:start/=1000.0
    if end>1e12:end/=1000.0
    # Explicitly reject markets whose lifecycle flags are contradictory.
    accepting=parse_bool(m.get('acceptingOrders'),None); book_enabled=parse_bool(m.get('enableOrderBook'),None)
    if accepting is None or book_enabled is None:return None
    return {'id':str(m.get('id') or condition),'condition':condition,'market':str(m.get('question') or m.get('title') or slug),'slug':slug,'asset':asset,'up':up,'down':down,'start_ts':start,'end_ts':float(end if end is not None else start+300.0),'raw':m,'accepting_orders':bool(accepting),'enable_order_book':bool(book_enabled)}

def get_by_slug(slug):
    r=SESSION.get(f'{GAMMA}/markets/slug/{slug}',timeout=10)
    if r.status_code==200 and isinstance(r.json(),dict):return r.json()
    if r.status_code not in (400,404):r.raise_for_status()
    r=SESSION.get(f'{GAMMA}/markets',params={'slug':slug},timeout=10); r.raise_for_status(); rs=rows(r); return rs[0] if rs else None

def discover(now=None,lookahead=600):
    now=time.time() if now is None else float(now); base=int(now//300)*300; out={}; diag=[]
    for asset,prefix in ASSETS.items():
        if asset=='HYPE' and os.getenv('V21_SIX_ASSET_MODE','true').lower() not in ('1','true','yes','on'):continue
        found=False
        starts=range(base-300, base+int(lookahead)+301, 300)
        for start in starts:
            slug=f'{prefix}-updown-5m-{start}'
            try:raw=get_by_slug(slug)
            except requests.RequestException as exc:diag.append(f'{asset}:{slug}:HTTP:{type(exc).__name__}');continue
            if not raw:diag.append(f'{asset}:{slug}:MISS');continue
            m=normalize(raw)
            if not m:diag.append(f'{asset}:{slug}:INVALID');continue
            if not m['accepting_orders'] or not m['enable_order_book']:diag.append(f'{asset}:{slug}:NOT_TRADEABLE');continue
            if m['end_ts']<now-1 or m['start_ts']>now+lookahead:continue
            out[m['condition']]=m; diag.append(f'{asset}:{slug}:FOUND'); found=True; break
        if not found:diag.append(f'{asset}:NO_CURRENT_MARKET')
    LOG.info('DISCOVERY | discovered=%d | %s',len(out),' | '.join(diag))
    return list(out.values())

def book(token):
    r=SESSION.get(f'{CLOB}/book',params={'token_id':str(token)},timeout=5); r.raise_for_status(); d=r.json()
    def levels(xs,side):
        vals=[]
        for x in xs or []:
            try:p=float(x.get('price') if isinstance(x,dict) else x[0]); s=float(x.get('size') if isinstance(x,dict) else x[1])
            except (TypeError,ValueError,IndexError,KeyError):continue
            if 0<p<1 and s>=0:vals.append((p,s))
        if not vals:return None,0.0
        return (max(vals,key=lambda z:z[0]) if side=='bid' else min(vals,key=lambda z:z[0]))
    b=levels(d.get('bids'),'bid'); a=levels(d.get('asks'),'ask'); return b[0] if b[0] else None,a[0] if a[0] else None,b[1],a[1]

def resolve(market):
    raw=get_by_slug(market['slug'])
    if not raw:return None,None,'NOT_FOUND'
    objs=raw.get('tokens')
    if isinstance(objs,list):
        for x in objs:
            if isinstance(x,dict) and x.get('winner') is True:
                tok=x.get('token_id') or x.get('tokenId'); out=x.get('outcome')
                if tok:return str(tok),str(out or ''),'WINNER'
    tokens=parse_list(raw.get('clobTokenIds') or raw.get('clob_token_ids')); outcomes=parse_list(raw.get('outcomes')); prices=parse_list(raw.get('outcomePrices'))
    if len(tokens)==len(outcomes)==len(prices):
        for tok,out,px in zip(tokens,outcomes,prices):
            try:
                if float(px)>=0.999999:return str(tok),str(out),'PRICE'
            except (TypeError,ValueError):pass
    return None,None,'PENDING'
