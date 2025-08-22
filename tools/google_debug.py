import traceback
import json
import time
from scraper import google_search as gs
from scraper.config import config

queries = [
    "bluetec haman co.; ltd.",  # observed failing
    "daf trucks n.v.",           # observed succeeding
    "speedy fuels",              # observed failing
]

out = []

for q in queries:
    record = {"query": q, "rest": {}, "clientlib": {}, "raw": {}}
    print('\n=== QUERY:', q)

    # REST via google_client._rest_search
    try:
        t0 = time.time()
        items = gs.google_client._rest_search(q, 3)
        dt = time.time() - t0
        print('REST: ok, items=', len(items) if items is not None else None, 'time=', dt)
        record['rest']['ok'] = True
        record['rest']['items'] = len(items) if items is not None else None
        record['rest']['time'] = dt
    except Exception as e:
        print('REST: exception:', type(e).__name__, repr(e))
        print(traceback.format_exc())
        record['rest']['ok'] = False
        record['rest']['error'] = repr(e)
        record['rest']['trace'] = traceback.format_exc()

    # Raw requests GET to same endpoint
    try:
        import requests
        s = requests.Session()
        # Use system proxy settings
        params = {"q": q, "cx": gs.CX_ID, "key": gs.API_KEY, "num": 3}
        t0 = time.time()
        r = s.get('https://customsearch.googleapis.com/customsearch/v1', params=params, timeout=config.request_timeout, verify=not config.insecure_ssl)
        dt = time.time() - t0
        print('RAW GET: status=', r.status_code, 'time=', dt)
        record['raw']['ok'] = True
        record['raw']['status'] = r.status_code
        record['raw']['time'] = dt
        try:
            record['raw']['json_keys'] = list(r.json().keys())
        except Exception:
            record['raw']['json_error'] = traceback.format_exc()
    except Exception as e:
        print('RAW GET: exception:', type(e).__name__, repr(e))
        print(traceback.format_exc())
        record['raw']['ok'] = False
        record['raw']['error'] = repr(e)
        record['raw']['trace'] = traceback.format_exc()

    # Client library path
    try:
        gs.google_client._initialize_service()
        t0 = time.time()
        resp = gs.google_client._service.cse().list(q=q, cx=gs.CX_ID, num=3).execute()
        dt = time.time() - t0
        items = resp.get('items', [])
        print('CLIENTLIB: ok, items=', len(items), 'time=', dt)
        record['clientlib']['ok'] = True
        record['clientlib']['items'] = len(items)
        record['clientlib']['time'] = dt
    except Exception as e:
        print('CLIENTLIB: exception:', type(e).__name__, repr(e))
        print(traceback.format_exc())
        record['clientlib']['ok'] = False
        record['clientlib']['error'] = repr(e)
        record['clientlib']['trace'] = traceback.format_exc()

    out.append(record)

print('\n=== SUMMARY JSON ===')
print(json.dumps(out, indent=2))
