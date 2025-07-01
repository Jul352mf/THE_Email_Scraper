import logging
import os
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

class CanvasScraper:
    """Simple scraper for Canvas course modules."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def login(self, email: str, password: str) -> bool:
        """Attempt to login using the standard Canvas login form."""
        login_url = f"{self.base_url}/login"
        resp = self.session.get(login_url)
        if resp.status_code != 200:
            log.error("Failed to load login page: %s", resp.status_code)
            return False
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form")
        if not form:
            log.error("Login form not found")
            return False
        data = {inp.get("name"): inp.get("value", "") for inp in form.find_all("input") if inp.get("name")}
        data.update({"UserName": email, "Password": password})
        action = form.get("action")
        action = urljoin(login_url, action)
        resp = self.session.post(action, data=data)
        if resp.status_code != 200:
            log.error("Login failed: %s", resp.status_code)
            return False
        return True

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _demo()
