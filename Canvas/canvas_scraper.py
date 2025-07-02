import argparse
import logging
import os
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

log = logging.getLogger(__name__)

class CanvasScraper:
    """Simple scraper for Canvas course modules."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def login(self, email: str, password: str, *, headless: bool = True) -> bool:
        """Log in using Playwright to execute any required JavaScript."""
        login_url = f"{self.base_url}/login"
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=headless)
            page = browser.new_page()
            page.goto(login_url, wait_until="networkidle")
            page.fill("#userNameInput", email)
            page.fill("#passwordInput", password)
            page.click("#submitButton")
            page.wait_for_load_state("networkidle")

            if "login" in page.url:
                log.error("Login failed: still on login page")
                browser.close()
                return False

            for cookie in page.context.cookies():
                self.session.cookies.set(
                    cookie["name"],
                    cookie["value"],
                    domain=cookie.get("domain"),
                    path=cookie.get("path", "/"),
                )
            browser.close()
        return True

    def scrape_course(self, course_id: str, output_dir: str) -> List[str]:
        """Download all PDFs linked from a course's module pages."""
        html = self.fetch_modules_page(course_id)
        module_links = self.parse_module_links(html, self.base_url)
        downloaded: List[str] = []
        for link in module_links:
            resp = self.session.get(link)
            resp.raise_for_status()
            pdfs = self.parse_pdf_links(resp.text, self.base_url)
            for url, title in pdfs:
                path = self.download_file(url, output_dir)
                log.info("Downloaded %s -> %s", title, path)
                downloaded.append(path)
        return downloaded

    def fetch_modules_page(self, course_id: str) -> str:
        url = f"{self.base_url}/courses/{course_id}/modules"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def parse_module_links(html: str, base_url: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        container = soup.find(id="context_modules_sortable_container")
        if not container:
            return []
        links: List[str] = []
        for a in container.find_all("a", class_="item_link", href=True):
            links.append(urljoin(base_url, a['href']))
        return links

    @staticmethod
    def parse_pdf_links(html: str, base_url: str) -> List[Tuple[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[Tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            href = a['href']
            if href.lower().endswith('.pdf') or '/files/' in href:
                url = urljoin(base_url, href)
                title = a.get("title") or a.get_text(strip=True)
                results.append((url, title))
        return results

    def download_file(self, url: str, output_dir: str) -> str:
        os.makedirs(output_dir, exist_ok=True)
        resp = self.session.get(url, stream=True)
        resp.raise_for_status()
        filename = url.split('/')[-1].split('?')[0]
        path = os.path.join(output_dir, filename)
        with open(path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return path


def _demo() -> None:
    base = "https://learning.unisg.ch"
    scraper = CanvasScraper(base)
    module_html = open(os.path.join("Canvas", "Canvas_Corporate_Finance_Module_html_F12_export.html"), encoding="utf-8").read()
    links = CanvasScraper.parse_module_links(module_html, base)
    print("Found", len(links), "module links")

    page_html = open(os.path.join("Canvas", "Canvas_Corporate_Finance_Module_Page_html_F12_export.html"), encoding="utf-8").read()
    pdfs = CanvasScraper.parse_pdf_links(page_html, base)
    print("PDF links:")
    for url, title in pdfs:
        print(" -", title, "->", url)


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download PDFs from Canvas")
    parser.add_argument("--base-url", default="https://learning.unisg.ch")
    parser.add_argument("--course-id", help="Course ID for Canvas")
    parser.add_argument("--output-dir", default="downloads")
    parser.add_argument("--email", default=os.environ.get("CANVAS_EMAIL"))
    parser.add_argument("--password", default=os.environ.get("CANVAS_PASSWORD"))
    parser.add_argument("--demo", action="store_true", help="Run parser demo")
    parser.add_argument("--headful", action="store_true", help="Show browser during login")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    if args.demo:
        _demo()
        return

    if not (args.email and args.password and args.course_id):
        parser.error("--email, --password, and --course-id required for real mode")

    scraper = CanvasScraper(args.base_url)
    if not scraper.login(args.email, args.password, headless=not args.headful):
        parser.error("Login failed")

    scraper.scrape_course(args.course_id, args.output_dir)


if __name__ == "__main__":
    main()
