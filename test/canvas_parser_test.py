import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Canvas.canvas_scraper import CanvasScraper


def run_tests():
    base = "https://learning.unisg.ch"
    module_html_path = os.path.join("Canvas", "Canvas_Corporate_Finance_Module_html_F12_export.html")
    with open(module_html_path, encoding="utf-8") as fh:
        module_html = fh.read()
    links = CanvasScraper.parse_module_links(module_html, base)
    print("modules", len(links))

    page_html_path = os.path.join("Canvas", "Canvas_Corporate_Finance_Module_Page_html_F12_export.html")
    with open(page_html_path, encoding="utf-8") as fh:
        page_html = fh.read()
    pdfs = CanvasScraper.parse_pdf_links(page_html, base)
    for p in pdfs:
        print("PDF", p)


if __name__ == "__main__":
    run_tests()
