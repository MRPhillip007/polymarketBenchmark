"""Latency probe for a trading server: how fast does this machine see Polymarket, and how fast does it reach its API?
No account, no orders: public endpoints only. Copy this one file to the server, then:
    pip install requests websockets
    python latency_probe.py                 # 30 minutes (default)
    python latency_probe.py --minutes 5     # quick look
Measures:
  1. clock    - offset of this machine's clock against NTP (feed delays are read against it), at the start and the end
  2. API      - full request time to clob.polymarket.com over a kept-alive connection (/time, /book), a fresh connection
                (DNS + TCP + TLS), gamma-api; and which Cloudflare location answers. A burst every 5 minutes.
  3. market   - Polymarket market websocket: delay from the event time to its arrival here, BTC/ETH/SOL 15m + 5m books,
                one connection per coin (as the paper bot); calm seconds vs the busiest 5%
  4. RTDS     - Polymarket's Chainlink price stream: delay from the price time and from the send time; how many seconds
                after a 5m start the last price needed for its start price (the minute average) is here
  5. Binance  - aggTrade stream (data-stream.binance.vision): delay from Binance's event time (and whether it is reachable)
Writes latency_<host>_<UTC>.json with the numbers next to this file and prints a report."""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import os
import platform
import socket
import statistics
import struct
import sys
import threading
import time
from datetime import datetime, timezone

try:
    import requests
    import websockets
except ImportError:
    sys.exit('needs: pip install requests websockets   (optional, faster: orjson)')
try:
    import orjson
    loads = orjson.loads
except ImportError:
    loads = json.loads

CLOB = 'https://clob.polymarket.com'
GAMMA = 'https://gamma-api.polymarket.com'
MARKET_WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
RTDS_WS = 'wss://ws-live-data.polymarket.com'
BINANCE_WS = 'wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade'
COINS = ('btc', 'eth', 'sol')
NTP_SERVERS = ('time.google.com', 'time.cloudflare.com', 'pool.ntp.org')
WARMUP_MS = 3000            # events right after (re)subscribing are snapshots with old times: not counted
HTTP_EVERY_S = 300

OFFSET_S = 0.0              # add to time.time() to get NTP time


def now_ms() -> float:
    return (time.time() + OFFSET_S) * 1000


def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


# ------------------------------------------------------------------ helpers

def pct(xs, q):
    if not xs:
        return float('nan')
    s = sorted(xs); k = (len(s) - 1) * q; f = int(k); c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def summ(xs) -> dict:
    return {'n': len(xs), 'min': min(xs) if xs else float('nan'), 'p10': pct(xs, .1), 'median': pct(xs, .5),
            'p90': pct(xs, .9), 'p99': pct(xs, .99), 'max': max(xs) if xs else float('nan')}


def line(label, xs, unit='ms'):
    s = summ(xs)
    if not s['n']:
        return f'  {label:<44} no data'
    return (f"  {label:<44} n={s['n']:<7} min {s['min']:8.1f}  median {s['median']:8.1f}  p90 {s['p90']:8.1f}  "
            f"p99 {s['p99']:8.1f}  max {s['max']:9.1f} {unit}")


# ------------------------------------------------------------------ 1. clock

def sntp(server: str, timeout: float = 2.0):
    """(offset s, round trip s) of one SNTP exchange; offset = server clock - this clock"""
    pkt = b'\x23' + 47 * b'\0'
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        t1 = time.time(); s.sendto(pkt, (server, 123)); data, _ = s.recvfrom(48); t4 = time.time()
    ts = lambda b: struct.unpack('!I', b[:4])[0] - 2208988800 + struct.unpack('!I', b[4:])[0] / 2 ** 32
    t2, t3 = ts(data[32:40]), ts(data[40:48])
    return ((t2 - t1) + (t3 - t4)) / 2, (t4 - t1) - (t3 - t2)


def clock_offset() -> dict:
    """per server: offset of the lowest-delay sample of 8; overall = median of the servers"""
    per = {}
    for srv in NTP_SERVERS:
        best = None
        for _ in range(8):
            try:
                o, d = sntp(srv)
                if best is None or d < best[1]:
                    best = (o, d)
            except OSError:
                pass
            time.sleep(0.05)
        if best:
            per[srv] = {'offset_ms': best[0] * 1000, 'rtt_ms': best[1] * 1000}
    off = statistics.median([v['offset_ms'] for v in per.values()]) if per else None
    return {'servers': per, 'offset_ms': off}


# ------------------------------------------------------------------ markets

def discover(minutes: float) -> list[dict]:
    """BTC/ETH/SOL 15m and 5m windows running now or starting during the test (Gamma API)"""
    now = int(time.time()); slugs = []
    for tf, step in (('15m', 900), ('5m', 300)):
        s = now // step * step
        while s < now + minutes * 60 + step:
            slugs += [f'{c}-updown-{tf}-{s}' for c in COINS]
            s += step
    out = []
    for i in range(0, len(slugs), 40):
        r = requests.get(f'{GAMMA}/events', params=[('slug', x) for x in slugs[i:i + 40]] + [('limit', 500)], timeout=20)
        r.raise_for_status()
        for e in r.json():
            for m in e.get('markets', []):
                toks, outs = json.loads(m['clobTokenIds']), json.loads(m['outcomes'])
                out.append({'slug': e['slug'], 'coin': e['slug'].split('-')[0], 'tf': e['slug'].split('-')[2],
                            'start': int(e['slug'].rsplit('-', 1)[1]), 'up': toks[outs.index('Up')], 'down': toks[outs.index('Down')]})
    return out


# ------------------------------------------------------------------ 2. API

def current_book_token(mk: list[dict]) -> str:
    """Up token of the BTC 15m window running now (a closed window's book is gone)"""
    now = time.time()
    cur = [m for m in mk if m['coin'] == 'btc' and m['tf'] == '15m' and m['start'] <= now < m['start'] + 900]
    return (cur or [m for m in mk if m['coin'] == 'btc'])[-1]['up']


def http_burst(st: dict, book_token: str, n: int = 20, gap: float = 0.25) -> None:
    sess = st['_sess']
    for path, key in (('/time', 'time'), (f'/book?token_id={book_token}', 'book')):
        for _ in range(n):
            try:
                t0 = time.perf_counter(); r = sess.get(CLOB + path, timeout=10); ms = (time.perf_counter() - t0) * 1000
                if r.status_code == 200:
                    st['http'][key].append(ms)
                st['colo'][r.headers.get('cf-ray', '-').split('-')[-1]] += 1
                st['cache'][f"{key}:{r.headers.get('cf-cache-status', '-')}"] += 1
            except requests.RequestException as err:
                st['errors'].append(f'http {key}: {type(err).__name__}')
            time.sleep(gap)
    for _ in range(3):                                     # a new connection each time: DNS + TCP + TLS + request
        try:
            with requests.Session() as s:
                t0 = time.perf_counter(); s.get(CLOB + '/time', timeout=10); st['http']['fresh'].append((time.perf_counter() - t0) * 1000)
        except requests.RequestException as err:
            st['errors'].append(f'http fresh: {type(err).__name__}')
        time.sleep(gap)
    for _ in range(5):
        try:
            t0 = time.perf_counter()
            sess.get(f'{GAMMA}/events', params={'slug': st['_gamma_slug'], '_': str(time.time())}, timeout=10)
            st['http']['gamma'].append((time.perf_counter() - t0) * 1000)
        except requests.RequestException as err:
            st['errors'].append(f'http gamma: {type(err).__name__}')
        time.sleep(gap)


def http_loop(st: dict, mk: list[dict], stop: float) -> None:
    st['_sess'] = requests.Session()
    while time.time() < stop - 30:
        http_burst(st, current_book_token(mk))
        nxt = time.time() + HTTP_EVERY_S
        while time.time() < min(nxt, stop - 30):
            time.sleep(1)


# ------------------------------------------------------------------ 3. market websocket

async def market_ws(coin: str, mk: list[dict], stop: float, st: dict) -> None:
    """the coin's current 15m and current 5m market, as a bot would hold them; a new connection at every 5m start"""
    while time.time() < stop:
        now = time.time(); b5, b15 = int(now // 300) * 300, int(now // 900) * 900
        toks = [t for m in mk if m['coin'] == coin and ((m['tf'] == '15m' and m['start'] == b15) or (m['tf'] == '5m' and m['start'] == b5))
                for t in (m['up'], m['down'])]
        until = min(stop, b5 + 300.5)
        if not toks:
            await asyncio.sleep(1); continue
        try:
            # max_queue=None: the library keeps reading the socket even if this loop falls behind for a moment; with the
            # default (16 messages) it stops reading, the server's send buffer fills and it drops us ('slow consumer')
            async with websockets.connect(MARKET_WS, ping_interval=None, max_size=None, max_queue=None, open_timeout=10) as ws:
                await ws.send(json.dumps({'assets_ids': toks, 'type': 'market'}))
                sub = now_ms(); next_ping = time.time() + 10
                while time.time() < until:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, min(next_ping, until) - time.time()))
                    except asyncio.TimeoutError:
                        raw = None
                    if raw:
                        t = now_ms()
                        st['bytes'][coin] += len(raw)
                        if raw.strip().upper() != 'PONG' and t - sub > WARMUP_MS:
                            try:
                                msg = loads(raw)
                            except ValueError:
                                msg = []
                            for x in msg if isinstance(msg, list) else [msg]:
                                if isinstance(x, dict) and x.get('timestamp'):
                                    ts = int(x['timestamp'])
                                    st['mkt'].append((coin, x.get('event_type', '?'), ts // 1000, t - ts))
                    if time.time() >= next_ping:
                        await ws.send('PING'); next_ping = time.time() + 10
        except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as err:
            st['reconnects'][f'market {coin}'] += 1; st['errors'].append(f'market {coin}: {type(err).__name__}: {err}')
            st['drops'].append({'utc': utc(time.time()), 'coin': coin, 'error': f'{type(err).__name__}: {err}'[:160]})
            await asyncio.sleep(1)


# ------------------------------------------------------------------ 4. RTDS (Chainlink)

async def rtds_ws(stop: float, st: dict) -> None:
    sub = {'action': 'subscribe', 'subscriptions': [{'topic': 'crypto_prices_chainlink', 'type': '*'}]}   # no filter: a filter
    want = {f'{c}/usd' for c in COINS}                                                                      # gives only history
    while time.time() < stop:
        try:
            async with websockets.connect(RTDS_WS, ping_interval=5, max_size=None, max_queue=None, open_timeout=10) as ws:
                await ws.send(json.dumps(sub))
                while time.time() < stop:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, stop - time.time()))
                    except asyncio.TimeoutError:
                        continue
                    t = now_ms()
                    try:
                        m = json.loads(raw)
                    except ValueError:
                        continue
                    p = m.get('payload') if isinstance(m, dict) else None
                    if not isinstance(p, dict) or p.get('symbol') not in want or 'timestamp' not in p:
                        continue
                    pts = int(p['timestamp'])
                    st['rtds_price'].append(t - pts)
                    if m.get('timestamp'):
                        st['rtds_send'].append(t - int(m['timestamp']))
                    if pts % 300_000 == 299_000:                    # the last second of the minute before a 5m start
                        st['rtds_ready'].append((t - (pts + 1000)) / 1000)
        except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as err:
            st['reconnects']['rtds'] += 1; st['errors'].append(f'rtds: {type(err).__name__}: {err}')
            await asyncio.sleep(1)


# ------------------------------------------------------------------ 5. Binance

async def binance_ws(stop: float, st: dict) -> None:
    while time.time() < stop:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20, max_size=None, max_queue=None, open_timeout=10) as ws:
                while time.time() < stop:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, stop - time.time()))
                    except asyncio.TimeoutError:
                        continue
                    t = now_ms()
                    d = json.loads(raw).get('data', {})
                    if 'E' in d:
                        st['binance'].append(t - int(d['E']))
        except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as err:
            st['reconnects']['binance'] += 1; st['errors'].append(f'binance: {type(err).__name__}: {err}')
            await asyncio.sleep(5)


async def progress(stop: float, st: dict, t0: float) -> None:
    while time.time() < stop:
        await asyncio.sleep(min(60, max(0.1, stop - time.time())))
        recent = [d for _, _, s, d in st['mkt'][-5000:]]
        print(f'  [{(time.time() - t0) / 60:4.1f} min] market events {len(st["mkt"])}, median delay {pct(recent, .5):.0f} ms | '
              f'Chainlink {len(st["rtds_price"])} | Binance {len(st["binance"])} | API requests {len(st["http"]["book"])}', flush=True)


# ------------------------------------------------------------------ main

def main() -> None:
    global OFFSET_S
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--minutes', type=float, default=30)
    ap.add_argument('--no-binance', action='store_true')
    args = ap.parse_args()
    host = socket.gethostname()
    print(f'latency probe on {host} ({platform.system()} {platform.release()}, Python {platform.python_version()}), '
          f'{args.minutes:g} min, start {utc(time.time())} UTC', flush=True)

    clk0 = clock_offset()
    if clk0['offset_ms'] is None:
        print('  ! NTP not reachable (UDP 123 blocked?): delays are read against this machine\'s own clock', flush=True)
    else:
        OFFSET_S = clk0['offset_ms'] / 1000
        per = ', '.join('%s %+.1f' % (k, v['offset_ms']) for k, v in clk0['servers'].items())
        side = 'behind' if clk0['offset_ms'] > 0 else 'ahead of'
        print(f"  clock: this machine is {abs(clk0['offset_ms']):.1f} ms {side} NTP ({per}); corrected for", flush=True)

    mk = discover(args.minutes)
    if not mk:
        sys.exit('no markets found on gamma-api')
    print(f'  markets for the test: {len(mk)} (each coin holds its current 15m + 5m market); '
          f"JSON parser: {'orjson' if loads is not json.loads else 'json (pip install orjson is faster)'}", flush=True)

    st = {'http': collections.defaultdict(list), 'colo': collections.Counter(), 'cache': collections.Counter(), 'mkt': [],
          'rtds_price': [], 'rtds_send': [], 'rtds_ready': [], 'binance': [], 'reconnects': collections.Counter(), 'errors': [],
          'drops': [], 'bytes': collections.Counter(), '_gamma_slug': mk[0]['slug']}
    t0 = time.time(); stop = t0 + args.minutes * 60; cpu0 = time.process_time()
    threading.Thread(target=http_loop, args=(st, mk, stop), daemon=True).start()

    async def run():
        jobs = [market_ws(c, mk, stop, st) for c in COINS] + [rtds_ws(stop, st), progress(stop, st, t0)]
        if not args.no_binance:
            jobs.append(binance_ws(stop, st))
        await asyncio.gather(*jobs)
    asyncio.run(run())
    wall, cpu = time.time() - t0, time.process_time() - cpu0
    clk1 = clock_offset()

    # ---------------- report
    print('\n================ REPORT ================')
    print(f'host {host}, {utc(t0)} - {utc(time.time())} UTC')
    if clk0['offset_ms'] is not None and clk1['offset_ms'] is not None:
        print(f"clock vs NTP: start {clk0['offset_ms']:+.1f} ms, end {clk1['offset_ms']:+.1f} ms "
              f"(drift {clk1['offset_ms'] - clk0['offset_ms']:+.1f} ms; NTP round trip "
              f"{min(v['rtt_ms'] for v in clk0['servers'].values()):.1f} ms = the accuracy limit of the delays below)")
    print(f"\nAPI clob.polymarket.com, Cloudflare location(s) answering: {dict(st['colo'])}; cache: {dict(st['cache'])}")
    print(line('kept-alive GET /time', st['http']['time']))
    print(line('kept-alive GET /book (reaches Polymarket)', st['http']['book']))
    print(line('fresh connection GET /time (DNS+TCP+TLS)', st['http']['fresh']))
    print(line('gamma-api GET event', st['http']['gamma']))

    ev = st['mkt']
    print('\nmarket websocket: Polymarket event time -> arrival here')
    print(line('all events', [d for *_, d in ev]))
    for c in COINS:
        print(line(f'  {c}', [d for cc, _, _, d in ev if cc == c]))
    for typ in sorted({e for _, e, _, _ in ev}):
        print(line(f'  {typ}', [d for _, e, _, d in ev if e == typ]))
    per_sec = collections.Counter(s for _, _, s, _ in ev)              # by the second the event HAPPENED
    calm = busy_d = []
    cut = float('nan')
    if per_sec:
        cut = pct(list(per_sec.values()), .95)
        busy = {s for s, n in per_sec.items() if n >= cut}
        calm = [d for _, _, s, d in ev if s not in busy]; busy_d = [d for _, _, s, d in ev if s in busy]
        print(line(f'  calm seconds (< {cut:.0f} events/s)', calm))
        print(line(f'  busiest 5% of seconds (>= {cut:.0f} events/s)', busy_d))
    mb = {c: st['bytes'][c] / 1e6 for c in COINS}
    print(f"  received: {', '.join(f'{c} {mb[c]:.0f} MB' for c in COINS)} = {sum(mb.values()) * 8 / wall:.1f} Mbit/s on average; "
          f"CPU of this script {cpu / wall:.0%} of one core (near 100% = Python cannot keep up, not the network)")

    print('\nChainlink stream (RTDS)')
    print(line('price time -> arrival here', st['rtds_price']))
    print(line('send time -> arrival here', st['rtds_send']))
    print(line('5m start price computable, s after start', st['rtds_ready'], 's'))
    if not args.no_binance:
        print('\nBinance aggTrade')
        print(line('event time -> arrival here', st['binance']) if st['binance'] else '  not reachable from here (or no data)')
    print(f"\nreconnects: {dict(st['reconnects']) or 'none'}; errors: {len(st['errors'])}")
    for e in st['errors'][:10]:
        print('  ', e)
    for d in st['drops'][:20]:
        print(f"   market connection dropped {d['utc']} UTC ({d['coin']})")
    m_med, b_med = pct([d for *_, d in ev], .5), pct(st['http']['book'], .5)
    print(f'\nROUGH TOTAL (medians): see the event {m_med:.0f} ms after it happens + request round trip {b_med:.0f} ms '
          f'(+ Polymarket\'s 50 ms taker delay, unofficial) -> about {m_med + b_med / 2 + 50:.0f} ms from the event to the match')

    out = {'host': host, 'system': f'{platform.system()} {platform.release()}', 'python': platform.python_version(),
           'start_utc': utc(t0), 'minutes': args.minutes, 'clock_start': clk0, 'clock_end': clk1,
           'http': {k: summ(v) for k, v in st['http'].items()}, 'http_raw': dict(st['http']), 'colo': dict(st['colo']),
           'cache': dict(st['cache']), 'market_all': summ([d for *_, d in ev]),
           'market_by_coin': {c: summ([d for cc, _, _, d in ev if cc == c]) for c in COINS},
           'market_raw_sample': [d for *_, d in ev][::max(1, len(ev) // 50000)],
           'rtds_price': summ(st['rtds_price']), 'rtds_send': summ(st['rtds_send']), 'rtds_ready_s': st['rtds_ready'],
           'binance': summ(st['binance']), 'reconnects': dict(st['reconnects']), 'errors': st['errors'][:200],
           'market_mb': {c: st['bytes'][c] / 1e6 for c in COINS}, 'cpu_share_of_one_core': cpu / wall,
           'market_calm': summ(calm), 'market_busiest5pct': summ(busy_d), 'busy_cut_events_per_s': cut,
           'market_drops': st['drops'], 'json_parser': 'orjson' if loads is not json.loads else 'json'}
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        f"latency_{host}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=1, default=float)
    print(f'\nsaved {path}')


if __name__ == '__main__':
    main()
