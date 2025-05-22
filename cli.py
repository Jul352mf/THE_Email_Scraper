import argparse
from pathlib import Path
from .pipeline import scrape_companies

def main() -> None:
    ap = argparse.ArgumentParser(description="Email scraper")
    ap.add_argument("src", help="Input Excel file")
    ap.add_argument("dst", help="Output Excel file")
    args = ap.parse_args()
    scrape_companies(Path(args.src), Path(args.dst))


if __name__ == "__main__":
    # Ensure Ctrl-C is handled
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if len(sys.argv) < 3:
        sys.exit("Usage: python email_scraper.py leads.xlsx output.xlsx [-v]")
    try:
        scrape_companies(sys.argv[1], sys.argv[2])
    except KeyboardInterrupt:
        log.warning("Execution interrupted by user")
        sys.exit(1)