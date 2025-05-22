#!/usr/bin/env python3
# Most functional Version until now !!!!!!!
# Todos:
# 1. Consumer / Producer pattern (if needed for performance)
# 2. If no domain found for a company, use the first Google result if result name is similar to the company name
# 3. Case‑insensitive `mailto:` detection.
# 4. Refactor to multiple files.
# 5. Make it so it updates the input file with the found emails.
# 6. If no email found still save the domain.
 

from __future__ import annotations

import os, sys, re, time, logging, threading, signal
from pathlib import Path
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict, deque

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rapidfuzz import fuzz




