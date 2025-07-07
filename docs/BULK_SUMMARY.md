# Bulk Domain Summary

This mode collects high‑level page information for a list of companies.

```
python cli.py companies.xlsx output.json --bulk-domain-summary
```

Each entry in the JSON output contains:

- `company`: Name from the spreadsheet
- `domain`: Detected domain
- `page_count`: Number of pages visited
- `used_sitemap`: whether any sitemaps were parsed
- `pages`: list of pages with the fields `url`, `title`, `meta_description`, `meta_keywords` and the first 1000 characters of page text

If the spreadsheet includes a `Domain` column, the tool will use that domain directly without performing a Google search.
