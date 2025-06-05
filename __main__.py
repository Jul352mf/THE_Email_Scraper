#!/usr/bin/env python3
"""
Main entry point for the email scraper package.
"""


import traceback, sys
from multiprocessing import freeze_support
from scraper.cli import main

if __name__ == '__main__':
    freeze_support()
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted by user\n")
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
