import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin, urlparse
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading

PUBLIC_ROOT = "../public"
IMAGE_DIRECTORY = os.path.join(PUBLIC_ROOT, "images", "news")
IMAGE_WEB_PATH = "/images/news/"
OUTPUT_JSON = "news_data.json"

MAX_WORKERS = 10
MAX_CONSECUTIVE_EMPTY_PAGES = 3
IMAGE_DOWNLOAD_WORKERS = 5

# Thread-local storage for sessions
thread_local = threading.local()


def get_session():
    """Get or create a session for the current thread"""
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        retry = Retry(
            total=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)
        )
        adapter = HTTPAdapter(
            max_retries=retry, pool_connections=20, pool_maxsize=20
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
            }
        )
        thread_local.session = session
    return thread_local.session


def setup_directories():
    if not os.path.exists(IMAGE_DIRECTORY):
        os.makedirs(IMAGE_DIRECTORY)
        print(f"Created image directory: {IMAGE_DIRECTORY}")


def download_image(image_url):
    if not image_url:
        return None
    try:
        image_name = (
            f"{hash(image_url)}_"
            f"{os.path.basename(urlparse(image_url).path)}"
        )
        filename = os.path.join(IMAGE_DIRECTORY, image_name)

        if os.path.exists(filename):
            return filename

        session = get_session()
        response = session.get(image_url, stream=True, timeout=15)
        response.raise_for_status()

        with open(filename, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return filename
    except Exception as e:
        print(f"    - Error downloading image {image_url}: {e}")
        return None


def scrape_article_details(article_url, base_url):
    try:
        session = get_session()
        response = session.get(article_url, timeout=15)
        soup = BeautifulSoup(response.content, "lxml")

        # Extract title
        title_tag = soup.find("h1", class_="entry-title")
        title = title_tag.text.strip() if title_tag else "No Title Found"

        # Extract content
        content_div = soup.find("div", class_="entry-content")
        if content_div:
            for s in content_div(["script", "style"]):
                s.decompose()
            content_html = str(content_div)
        else:
            content_html = ""

        # Extract tags
        tags = []
        tags_span = soup.find("span", class_="tags-links")
        if tags_span:
            tag_links = tags_span.find_all("a", rel="tag")
            tags = [a.text.strip() for a in tag_links]

        # Extract images
        image_urls = []
        if content_div:
            for img_tag in content_div.find_all("img"):
                if img_src := img_tag.get("src"):
                    full_img_url = urljoin(base_url, img_src)
                    image_urls.append(full_img_url)

        # Extract published date
        published_date = ""
        date_element = soup.find("time", class_="entry-date published")
        if date_element and date_element.get("datetime"):
            datetime_str = date_element.get("datetime")
            try:
                dt = datetime.datetime.fromisoformat(
                    datetime_str.replace("Z", "+00:00")
                )
                published_date = dt.date().isoformat()
            except Exception as e:
                print(
                    f"    - Error parsing datetime '{datetime_str}': {e}"
                )

        return {
            "title": title,
            "content_html": content_html,
            "tags": tags,
            "image_urls": image_urls,
            "published_date": published_date,
        }
    except Exception as e:
        print(f"  - Error scraping details from {article_url}: {e}")
        return None


def process_article(article_data, article_counter):
    """Process a single article"""
    (
        post_id,
        article_url,
        thumbnail_url,
        published_date,
        base_url,
    ) = article_data

    details = scrape_article_details(article_url, base_url)
    if not details:
        print(f"  - Failed to scrape details for {article_url}")
        return None

    if not details.get("title") or details["title"] == "No Title Found":
        print(f"  - Skipping article with no title: {article_url}")
        return None
    if not details.get("content_html"):
        print(f"  - Skipping article with no content: {article_url}")
        return None

    # Download thumbnail
    thumbnail_local_path = (
        download_image(thumbnail_url) if thumbnail_url else None
    )
    thumbnail_web_path = (
        f"{IMAGE_WEB_PATH}{os.path.basename(thumbnail_local_path)}"
        if thumbnail_local_path
        else "/images/placeholder.jpg"
    )

    # Download content images in parallel and replace URLs
    processed_content = details["content_html"]
    with ThreadPoolExecutor(
        max_workers=IMAGE_DOWNLOAD_WORKERS
    ) as executor:
        future_to_url = {
            executor.submit(download_image, url): url
            for url in details["image_urls"]
        }
        for future in as_completed(future_to_url):
            original_url = future_to_url[future]
            local_path = future.result()
            if local_path:
                web_path = (
                    f"{IMAGE_WEB_PATH}{os.path.basename(local_path)}"
                )
                processed_content = processed_content.replace(
                    original_url, web_path
                )

    # Data preparation
    slug = generate_slug(details["title"])
    article_date = details.get("published_date", "")
    if not article_date:
        article_date = published_date
    parsed_date = (
        article_date
        if article_date
        else datetime.datetime.now().date().isoformat()
    )

    return {
        "id": str(article_counter),
        "slug": slug,
        "title": details["title"],
        "subtitle": "",
        "thumbnail": thumbnail_web_path,
        "tags": details["tags"],
        "content": processed_content,
        "publishedAt": parsed_date,
        "author": "SMKN 2 Singosari",
    }


def generate_slug(title):
    """Generate a URL-friendly slug from a title."""
    slug = re.sub(r"[^\w\s-]", "", title).strip().lower()
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug


def scrape_page(page, news_category_url, base_url, month_map):
    """Scrape a single page and return list of article tasks"""
    url = (
        f"{news_category_url}&paged={page}"
        if page > 1
        else news_category_url
    )

    try:
        session = get_session()
        list_response = session.get(url, timeout=15)
        list_response.raise_for_status()
        list_soup = BeautifulSoup(list_response.content, "lxml")

        primary_div = list_soup.find("div", id="primary")

        if not primary_div:
            return None, f"No primary div on page {page}"

        articles_on_page = primary_div.find_all(
            "article", class_=re.compile(r"\bpost-\d+\b")
        )

        if not articles_on_page:
            return [], f"No articles on page {page}"

        tasks = []
        for article_summary in articles_on_page:
            try:
                post_id_str = article_summary.get("id", "").replace(
                    "post-", ""
                )
                if not post_id_str.isdigit():
                    continue

                post_id = int(post_id_str)

                article_link_tag = article_summary.find(
                    "h2", class_="entry-title"
                )
                if not article_link_tag:
                    continue

                article_link = article_link_tag.find("a")
                if not article_link or not article_link.get("href"):
                    continue

                article_url = article_link["href"]

                thumbnail_tag = article_summary.find(
                    "img", class_="wp-post-image"
                )
                thumbnail_url = (
                    thumbnail_tag["src"] if thumbnail_tag else None
                )

                # Extract date from list page
                date_div = article_summary.find(
                    "div", class_="custom-entry-date"
                )
                if date_div:
                    month_span = date_div.find(
                        "span", class_="entry-month"
                    )
                    day_span = date_div.find("span", class_="entry-day")

                    if month_span and day_span:
                        month_str = month_span.text.strip()
                        day_str = day_span.text.strip().zfill(2)
                        month_num = month_map.get(month_str, "01")
                        published_date = f"YYYY-{month_num}-{day_str}"
                    else:
                        published_date = ""
                else:
                    published_date = ""

                tasks.append(
                    (
                        post_id,
                        article_url,
                        thumbnail_url,
                        published_date,
                        base_url,
                    )
                )

            except Exception as e:
                print(f"  -> Error processing article on page {page}: {e}")
                continue

        return tasks, None

    except requests.exceptions.RequestException as e:
        return None, f"Network error on page {page}: {e}"
    except Exception as e:
        return None, f"Unexpected error on page {page}: {e}"


def main():
    """Main function to run the scraper."""
    setup_directories()

    base_url = "https://smkn2-singosari.sch.id/"
    news_category_url = f"{base_url}?cat=4"

    month_map = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "Mei": "05",
        "Jun": "06",
        "Jul": "07",
        "Agu": "08",
        "Sep": "09",
        "Okt": "10",
        "Nov": "11",
        "Des": "12",
    }

    # Load existing data if file exists
    existing_slugs = set()
    article_counter = 1
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            existing_slugs = {item["slug"] for item in existing_data}
            article_counter = len(existing_data) + 1
        print(f"Found {len(existing_slugs)} existing articles in JSON file.")
    else:
        existing_data = []

    # Phase 1: Scrape all pages in parallel to get article URLs
    print("\n=== Phase 1: Scanning pages for article URLs ===")
    all_tasks = []
    consecutive_empty_pages = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        page_futures = {
            executor.submit(
                scrape_page, page, news_category_url, base_url, month_map
            ): page
            for page in range(1, 200)
        }

        for future in as_completed(page_futures):
            page = page_futures[future]
            try:
                tasks, error = future.result()

                if error:
                    print(f"Page {page}: {error}")
                    consecutive_empty_pages += 1
                    if (
                        consecutive_empty_pages
                        >= MAX_CONSECUTIVE_EMPTY_PAGES
                    ):
                        print(
                            f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} "
                            f"consecutive pages with errors/no content."
                        )
                        for f in page_futures:
                            f.cancel()
                        break
                    continue

                if tasks is None:
                    consecutive_empty_pages += 1
                    if (
                        consecutive_empty_pages
                        >= MAX_CONSECUTIVE_EMPTY_PAGES
                    ):
                        print(
                            f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} "
                            f"consecutive pages with errors."
                        )
                        for f in page_futures:
                            f.cancel()
                        break
                    continue

                if len(tasks) == 0:
                    consecutive_empty_pages += 1
                    if (
                        consecutive_empty_pages
                        >= MAX_CONSECUTIVE_EMPTY_PAGES
                    ):
                        print(
                            f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} "
                            f"consecutive empty pages."
                        )
                        for f in page_futures:
                            f.cancel()
                        break
                    continue

                consecutive_empty_pages = 0
                all_tasks.extend(tasks)
                print(
                    f"Page {page}: Found {len(tasks)} articles "
                    f"(Total: {len(all_tasks)})"
                )

            except Exception as e:
                print(f"Page {page}: Unexpected error: {e}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                    print(
                        f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} "
                        f"consecutive errors."
                    )
                    for f in page_futures:
                        f.cancel()
                    break

    print(f"\n=== Found {len(all_tasks)} total articles to process ===")

    # Phase 2: Process articles in parallel
    print("\n=== Phase 2: Processing articles in parallel ===")
    processed_count = 0
    new_articles = []
    data_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(
                process_article, task, article_counter + i
            ): task
            for i, task in enumerate(all_tasks)
        }

        for future in as_completed(future_to_task):
            try:
                result = future.result()
                if result:
                    with data_lock:
                        if result["slug"] not in existing_slugs:
                            new_articles.append(result)
                            existing_slugs.add(result["slug"])
                            processed_count += 1
                            print(
                                f"  - Processed ({processed_count}): "
                                f"{result['title'][:50]}..."
                            )
            except Exception as e:
                print(f"Error processing article: {e}")

    # Combine existing and new articles
    all_articles = existing_data + new_articles

    # Sort by publishedAt descending
    all_articles.sort(key=lambda x: x["publishedAt"], reverse=True)

    # Save to JSON file
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    print(
        f"\n=== Scraping finished. Added {processed_count} new articles. "
        f"Total: {len(all_articles)} articles in {OUTPUT_JSON} ==="
    )


if __name__ == "__main__":
    main()
