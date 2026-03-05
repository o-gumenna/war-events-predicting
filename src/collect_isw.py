import re
import time
import json
import csv
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import date


# configuration and constants
catalog_url = "https://understandingwar.org/research/?_teams=russia-ukraine"
base_domain = "https://understandingwar.org"
output_path = "data/isw/isw_reports.json"
csv_output_path = "data/isw/isw_reports.csv"
total_pages = 60
request_delay = 1.5

# regex to find month and day in titles
title_date_regex = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})",
    re.IGNORECASE,
)

# regex to extract year from page content
year_regex = re.compile(r"\b(20\d{2})\b")

# list of reports that lack standard date formatting in titles
hardcoded_entries = [
    {
        "url": "https://understandingwar.org/research/russia-ukraine/russia-ukraine-warning-update-initial/",
        "date": date(2022, 2, 24),
    }
]

def scrape_text(soup):
    # extracts main article text and removes unnecessary elements

    # look for common isw content containers
    content_box = (
            soup.find("div", class_=re.compile(r"field--name-body|field-name-body|field-items")) or
            soup.find("article")
    )
    text = ""
    if content_box:
        # remove scripts, styles, and navigation links
        for tag in content_box(["script", "style", "nav", "footer"]):
            tag.decompose()
        text = content_box.get_text(separator="\n", strip=True)
    return text


def convert_json_to_csv(json_path, csv_path):
    # convert collected JSON reports to CSV format
    with open(json_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    fieldnames = ["date", "url", "full_text", "word_count"]

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(records)

    print(f"converted {len(records)} records to {csv_path}")


def main():
    # define date boundaries for filtering
    start_limit = date(2022, 2, 24)
    end_limit = date(2026, 3, 3)

    with sync_playwright() as pw:
        # launch browser and create a new session
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0")
        page = context.new_page()

        all_urls = set()
        months_map = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
                      "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}

        print("phase 1: collecting all candidate links...")

        # iterate through catalog pages to find report links
        for pg in range(1, total_pages + 1):
            try:
                page.goto(f"{catalog_url}&_paged={pg}", wait_until="networkidle", timeout=60000)
                soup = BeautifulSoup(page.content(), "html.parser")

                for a_tag in soup.find_all("a", href=True):
                    title_text = a_tag.get_text(strip=True)
                    # filter only for assessment reports
                    if "Russian Offensive Campaign Assessment" not in title_text:
                        continue

                    href = a_tag["href"].split("?")[0].rstrip("/")
                    full_url = base_domain + href if href.startswith("/") else href
                    all_urls.add(full_url)

                print(f"page {pg}: total candidates: {len(all_urls)}")
            except Exception as e:
                print(f"error on page {pg}: {e}")

            time.sleep(request_delay)

        print(f"\nphase 2: extracting dates and content from {len(all_urls)} pages...")

        records = []
        for i, url in enumerate(list(all_urls), 1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                soup = BeautifulSoup(page.content(), "html.parser")

                # extract month and day from the h1 title
                h1 = soup.find("h1")
                title_text = h1.get_text(strip=True) if h1 else ""
                date_match = title_date_regex.search(title_text)

                if not date_match:
                    print(f"[{i}/{len(all_urls)}] skipped (no month/day in title): {url}")
                    continue

                month_num = months_map[date_match.group(1).lower()]
                day = int(date_match.group(2))

                # find the year in the article body or subheaders
                year = None
                for el in soup.find_all(string=year_regex):
                    match = year_regex.search(el)
                    if match:
                        year = int(match.group(1))
                        break

                if year is None:
                    print(f"[{i}/{len(all_urls)}] skipped (no year found): {url}")
                    continue

                # create date object and validate range
                try:
                    report_date = date(year, month_num, day)
                except ValueError as e:
                    print(f"[{i}/{len(all_urls)}] skipping invalid date: {day}/{month_num}/{year} — {e}")
                    continue

                if not (start_limit <= report_date <= end_limit):
                    print(f"[{i}/{len(all_urls)}] skipped (out of range): {report_date}")
                    continue

                # extract text and save record
                text = scrape_text(soup)
                records.append({
                    "date": str(report_date),
                    "url": url,
                    "full_text": text,
                    "word_count": len(text.split())
                })
                print(f"[{i}/{len(all_urls)}] scraped: {report_date}")
            except Exception as e:
                print(f"[{i}/{len(all_urls)}] failed: {url}: {e}")
            time.sleep(request_delay)

        print(f"\nphase 3: scraping {len(hardcoded_entries)} hardcoded entries...")

        # process reports that couldn't be parsed automatically
        for entry in hardcoded_entries:
            url = entry["url"]
            report_date = entry["date"]
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                soup = BeautifulSoup(page.content(), "html.parser")
                text = scrape_text(soup)
                records.append({
                    "date": str(report_date),
                    "url": url,
                    "full_text": text,
                    "word_count": len(text.split())
                })
                print(f"hardcoded scraped: {report_date} — {url}")
            except Exception as e:
                print(f"hardcoded failed: {url}: {e}")
            time.sleep(request_delay)

        browser.close()

    # sort results by date and save to json
    records.sort(key=lambda x: x["date"])
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"success! saved {len(records)} reports to {output_path}")

    # convert json to csv
    convert_json_to_csv(output_path, csv_output_path)


if __name__ == "__main__":
    main()