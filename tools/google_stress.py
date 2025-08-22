"""Small stress tester that repeatedly calls the project's Google search client
and logs results and exceptions to a rotating log file to try to reproduce
intermittent TLS/SSL failures seen in production runs.

Run from project root with the venv active:
  .\.venv\Scripts\Activate.ps1
  python tools\google_stress.py
"""
import time
import json
import logging
from pathlib import Path

from scraper.google_search import google_client

LOG = logging.getLogger('google_stress')
LOG.setLevel(logging.DEBUG)
fh = logging.FileHandler('tools/google_stress.log')
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fh.setFormatter(fmt)
LOG.addHandler(fh)

QUERIES = [
    'bluetec haman co.; ltd.',
    'daf trucks n.v.',
    'speedy fuels',
]

def run_once():
    out = []
    for q in QUERIES:
        t0 = time.time()
        try:
            items = google_client.search(q, num_results=3)
            ok = True
            err = None
        except Exception as e:
            items = []
            ok = False
            err = repr(e)
        t1 = time.time()
        row = dict(query=q, ok=ok, items=len(items), err=err, time=t1-t0)
        out.append(row)
        LOG.info('RESULT %s', json.dumps(row))
    return out

def main():
    Path('tools').mkdir(exist_ok=True)
    LOG.info('Starting google_stress run')
    for i in range(200):
        out = run_once()
        # quick backoff between rounds
        time.sleep(0.5)
    LOG.info('Finished google_stress run')

if __name__ == '__main__':
    main()
