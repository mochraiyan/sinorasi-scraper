# import psycopg2
# import requests
# from bs4 import BeautifulSoup
# import os
# import re
# from urllib.parse import urljoin, urlparse
# import json
# import datetime
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# # --- CONFIGURATION ---
# # Database
# POSTGRES_URL = "postgresql://myuser:mypassword@localhost:5432/mydb"

# # Paths
# # The root of the web server's public directory, relative to this script
# PUBLIC_ROOT = "../public"
# IMAGE_DIRECTORY = os.path.join(PUBLIC_ROOT, 'images', 'news')
# # Web path to the image directory
# IMAGE_WEB_PATH = '/images/news/'

# # Performance
# MAX_WORKERS = 10
# BATCH_SIZE = 20
# # --- END CONFIGURATION ---

# def create_pg_connection():
#     """Create PostgreSQL connection using psycopg2"""
#     url = urlparse(POSTGRES_URL)
#     return psycopg2.connect(
#         host=url.hostname,
#         port=url.port,
#         database=url.path[1:],
#         user=url.username,
#         password=url.password
#     )

# def create_session():
#     """Create a requests session with connection pooling and retry logic"""
#     session = requests.Session()
#     retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 504))
#     adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)
#     return session

# # Create a shared session
# SESSION = create_session()

# def setup_directories():
#     """Ensure required directories exist."""
#     if not os.path.exists(IMAGE_DIRECTORY):
#         os.makedirs(IMAGE_DIRECTORY)
#         print(f"Created image directory: {IMAGE_DIRECTORY}")

# def download_image(image_url):
#     """Downloads an image and saves it to the IMAGE_DIRECTORY."""
#     if not image_url:
#         return None
#     try:
#         # Create a unique, safe filename
#         image_name = f"{hash(image_url)}_{os.path.basename(urlparse(image_url).path)}"
#         filename = os.path.join(IMAGE_DIRECTORY, image_name)

#         # Only download if it doesn't already exist
#         if os.path.exists(filename):
#             # print(f"    - Image already exists: {image_name}")
#             return filename

#         response = SESSION.get(image_url, stream=True, timeout=15)
#         response.raise_for_status()

#         with open(filename, 'wb') as f:
#             for chunk in response.iter_content(8192):
#                 f.write(chunk)
#         return filename
#     except Exception as e:
#         print(f"    - Error downloading image {image_url}: {e}")
#         return None

# def determine_category(article_url, tags):
#     """Determine the category based on URL and tags"""
#     url_lower = article_url.lower()
#     tags_lower = " ".join(tags).lower() if tags else ""
    
#     if "prestasi" in url_lower or "prestasi" in tags_lower or "achievement" in tags_lower:
#         return "achievements"
#     elif "event" in url_lower or "event" in tags_lower or "acara" in tags_lower or "kegiatan" in tags_lower:
#         return "events"
#     else:
#         return "general"

# def scrape_article_details(article_url, base_url):
#     """Scrapes all necessary details from a single article page."""
#     try:
#         response = SESSION.get(article_url, timeout=15)
#         soup = BeautifulSoup(response.content, 'lxml')

#         title_tag = soup.find('h1', class_='entry-title')
#         title = title_tag.text.strip() if title_tag else "No Title Found"

#         content_div = soup.find('div', class_='entry-content')
        
#         # Preserve HTML content and remove unwanted tags
#         if content_div:
#             # Remove script and style tags
#             for s in content_div(['script', 'style']):
#                 s.decompose()
#             content_html = str(content_div)
#         else:
#             content_html = ""

#         tags_span = soup.find('span', class_='tags-links')
#         tags = [a.text.strip() for a in tags_span.find_all('a')] if tags_span else []

#         # Find all image URLs within the article content
#         image_urls = []
#         if content_div:
#             for img_tag in content_div.find_all('img'):
#                 if img_src := img_tag.get('src'):
#                     full_img_url = urljoin(base_url, img_src)
#                     image_urls.append(full_img_url)
        
#         category = determine_category(article_url, tags)

#         return {
#             "title": title,
#             "content_html": content_html,
#             "tags": tags,
#             "image_urls": image_urls,
#             "category": category,
#         }
#     except Exception as e:
#         print(f"  - Error scraping details from {article_url}: {e}")
#         return None

# def process_article(article_data):
#     """
#     Processes a single article: scrapes, downloads images, and prepares data for DB insertion.
#     Returns a dictionary with all data ready for insertion, or None on failure.
#     """
#     post_id, article_url, thumbnail_url, published_date, base_url = article_data

#     details = scrape_article_details(article_url, base_url)
#     if not details:
#         print(f"  - Failed to scrape details for {article_url}")
#         return None

#     # Skip articles with no title or content
#     if not details.get('title') or details['title'] == "No Title Found":
#         print(f"  - Skipping article with no title: {article_url}")
#         return None
#     if not details.get('content_html'):
#         print(f"  - Skipping article with no content: {article_url}")
#         return None

#     # --- Image Processing ---
#     # Download thumbnail
#     thumbnail_local_path = download_image(thumbnail_url) if thumbnail_url else None
#     thumbnail_web_path = f"{IMAGE_WEB_PATH}{os.path.basename(thumbnail_local_path)}" if thumbnail_local_path else None

#     # Download content images and replace URLs in the content
#     processed_content = details['content_html']
#     with ThreadPoolExecutor(max_workers=5) as executor:
#         future_to_url = {executor.submit(download_image, url): url for url in details['image_urls']}
#         for future in as_completed(future_to_url):
#             original_url = future_to_url[future]
#             local_path = future.result()
#             if local_path:
#                 web_path = f"{IMAGE_WEB_PATH}{os.path.basename(local_path)}"
#                 # Replace the original URL in the HTML with the new local web path
#                 processed_content = processed_content.replace(original_url, web_path)

#     # --- Data Preparation ---
#     slug = generate_slug(details['title'])
#     parsed_date = parse_published_date(published_date)

#     return {
#         'post_id': post_id,
#         'slug': slug,
#         'title': details['title'],
#         'subtitle': "", # Subtitle not scraped
#         'thumbnail': thumbnail_web_path,
#         'tags': json.dumps(details['tags']), # Store as JSON string
#         'content': processed_content,
#         'published_at': parsed_date,
#         'author': "SMKN 2 Singosari", # Default author
#         'category': details['category'],
#         'url': article_url,
#     }

# def get_existing_post_ids(conn):
#     """Get all existing post IDs from PostgreSQL to avoid duplicates."""
#     with conn.cursor() as c:
#         # Assuming there's a 'post_id' column in the 'news' table to track original ID
#         # If not, we'll use the URL. Let's add a post_id column for robustness.
#         # For now, let's check by slug, which should be unique.
#         c.execute("SELECT slug FROM news")
#         # This is not ideal, as slugs can change. A `post_id` column is better.
#         # For this implementation, we will rely on the slug.
#         return set(row[0] for row in c.fetchall())

# def generate_slug(title):
#     """Generate a URL-friendly slug from a title."""
#     slug = re.sub(r'[^\\w\\s-]', '', title).strip().lower()
#     slug = re.sub(r'[\\s-]+', '-', slug)
#     return slug

# def parse_published_date(date_str):
#     """Parse various date string formats into a standard ISO format."""
#     if date_str and date_str.strip():
#         try:
#             # Handle "YYYY-MM-DD" format
#             if len(date_str) == 10 and date_str.count('-') == 2:
#                 return datetime.datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
#             # Handle "YYYY-MM-DD" with placeholder year - use 2024
#             elif 'YYYY-' in date_str:
#                 date_str = date_str.replace('YYYY', '2024')
#                 return datetime.datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
#         except ValueError:
#             pass # Fallback to now
#     return datetime.datetime.now().date().isoformat()

# def insert_batch(conn, batch):
#     """Insert a batch of articles into the PostgreSQL database."""
#     with conn.cursor() as c:
#         for article in batch:
#             # Using ON CONFLICT to prevent duplicates based on the unique slug
#             c.execute(
#                 """
#                 INSERT INTO news (slug, title, subtitle, thumbnail, tags, content, published_at, author)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                 ON CONFLICT (slug) DO NOTHING
#                 """,
#                 (
#                     article['slug'], article['title'], article['subtitle'],
#                     article['thumbnail'], article['tags'], article['content'],
#                     article['published_at'], article['author']
#                 )
#             )
#     conn.commit()
#     print(f"  -> Committed batch of {len(batch)} articles.")

# def main():
#     """Main function to run the scraper."""
#     setup_directories()
#     pg_conn = create_pg_connection()

#     base_url = "https://smkn2-singosari.sch.id/"
#     news_category_url = f"{base_url}?cat=4"

#     month_map = {
#         'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Mei': '05', 'Jun': '06',
#         'Jul': '07', 'Agu': '08', 'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
#     }

#     # Get existing post slugs to avoid re-scraping
#     existing_slugs = get_existing_post_ids(pg_conn)
#     print(f"Found {len(existing_slugs)} existing articles in PostgreSQL.")

#     all_tasks = []
#     for page in range(1, 100): # Increase pages to scrape all
#         url = f"{news_category_url}&paged={page}" if page > 1 else news_category_url
#         print(f"\nScanning page {page}: {url}")

#         try:
#             list_response = SESSION.get(url, timeout=15)
#             list_soup = BeautifulSoup(list_response.content, 'lxml')
#             articles_on_page = list_soup.find('div', id='primary').find_all('article', class_=re.compile(r'\bpost-\d+\b'))

#             print(f"  -> Found {len(articles_on_page)} articles on page {page}.")

#             if not articles_on_page:
#                 print(f"No more articles found on page {page}, stopping.")
#                 break

#             for article_summary in articles_on_page:
#                 post_id_str = article_summary.get('id', '').replace('post-', '')
#                 if not post_id_str.isdigit():
#                     continue

#                 post_id = int(post_id_str)

#                 article_link_tag = article_summary.find('h2', class_='entry-title').find('a')
#                 article_url = article_link_tag['href']

#                 # Basic check to skip existing articles based on URL (less reliable than slug)
#                 # A better check is done later with the generated slug.

#                 thumbnail_tag = article_summary.find('img', class_='wp-post-image')
#                 thumbnail_url = thumbnail_tag['src'] if thumbnail_tag else None

#                 date_div = article_summary.find('div', class_='custom-entry-date')
#                 if date_div:
#                     month_str = date_div.find('span', class_='entry-month').text.strip()
#                     day_str = date_div.find('span', class_='entry-day').text.strip().zfill(2)
#                     month_num = month_map.get(month_str, '01') # Default to Jan
#                     # Year is not available on the list page, so we use a placeholder
#                     published_date = f"YYYY-{month_num}-{day_str}"
#                 else:
#                     published_date = ""

#                 all_tasks.append((post_id, article_url, thumbnail_url, published_date, base_url))

#         except Exception as e:
#             print(f"  - Error on page {page}: {e}")
#             continue

#     print(f"\nFound {len(all_tasks)} total articles to check.")
#     print("Processing articles in parallel...")

#     processed_count = 0
#     new_articles_batch = []

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         future_to_task = {executor.submit(process_article, task): task for task in all_tasks}
        
#         for future in as_completed(future_to_task):
#             result = future.result()
#             if result:
#                 # Final check to avoid duplicates before adding to batch
#                 if result['slug'] not in existing_slugs:
#                     new_articles_batch.append(result)
#                     existing_slugs.add(result['slug']) # Add to set to handle in-run duplicates
#                     processed_count += 1
#                     print(f"  - Queued for insertion ({processed_count}): {result['title'][:50]}...")
                
#                 if len(new_articles_batch) >= BATCH_SIZE:
#                     insert_batch(pg_conn, new_articles_batch)
#                     new_articles_batch = []

#     # Insert any remaining articles in the last batch
#     if new_articles_batch:
#         insert_batch(pg_conn, new_articles_batch)

#     pg_conn.close()
#     print(f"\nScraping finished. Added {processed_count} new articles to the database.")

# if __name__ == '__main__':
#     main()


import psycopg2
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
import time

# --- CONFIGURATION ---
# Database
POSTGRES_URL = "postgresql://myuser:mypassword@localhost:5432/mydb"

# Paths
# The root of the web server's public directory, relative to this script
PUBLIC_ROOT = "../public"
IMAGE_DIRECTORY = os.path.join(PUBLIC_ROOT, 'images', 'news')
# Web path to the image directory
IMAGE_WEB_PATH = '/images/news/'

# Performance
MAX_WORKERS = 10
BATCH_SIZE = 20
MAX_CONSECUTIVE_EMPTY_PAGES = 3  # Stop after 3 consecutive empty pages
# --- END CONFIGURATION ---

def create_pg_connection():
    """Create PostgreSQL connection using psycopg2"""
    url = urlparse(POSTGRES_URL)
    return psycopg2.connect(
        host=url.hostname,
        port=url.port,
        database=url.path[1:],
        user=url.username,
        password=url.password
    )

def create_session():
    """Create a requests session with connection pooling and retry logic"""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 504))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    # Add a user agent to avoid being blocked
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session

# Create a shared session
SESSION = create_session()

def setup_directories():
    """Ensure required directories exist."""
    if not os.path.exists(IMAGE_DIRECTORY):
        os.makedirs(IMAGE_DIRECTORY)
        print(f"Created image directory: {IMAGE_DIRECTORY}")

def download_image(image_url):
    """Downloads an image and saves it to the IMAGE_DIRECTORY."""
    if not image_url:
        return None
    try:
        # Create a unique, safe filename
        image_name = f"{hash(image_url)}_{os.path.basename(urlparse(image_url).path)}"
        filename = os.path.join(IMAGE_DIRECTORY, image_name)

        # Only download if it doesn't already exist
        if os.path.exists(filename):
            # print(f"    - Image already exists: {image_name}")
            return filename

        response = SESSION.get(image_url, stream=True, timeout=15)
        response.raise_for_status()

        with open(filename, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return filename
    except Exception as e:
        print(f"    - Error downloading image {image_url}: {e}")
        return None

def determine_category(article_url, tags):
    """Determine the category based on URL and tags"""
    url_lower = article_url.lower()
    tags_lower = " ".join(tags).lower() if tags else ""
    
    if "prestasi" in url_lower or "prestasi" in tags_lower or "achievement" in tags_lower:
        return "achievements"
    elif "event" in url_lower or "event" in tags_lower or "acara" in tags_lower or "kegiatan" in tags_lower:
        return "events"
    else:
        return "general"

def scrape_article_details(article_url, base_url):
    """Scrapes all necessary details from a single article page."""
    try:
        response = SESSION.get(article_url, timeout=15)
        soup = BeautifulSoup(response.content, 'lxml')

        title_tag = soup.find('h1', class_='entry-title')
        title = title_tag.text.strip() if title_tag else "No Title Found"

        content_div = soup.find('div', class_='entry-content')

        # Preserve HTML content and remove unwanted tags
        if content_div:
            # Remove script and style tags
            for s in content_div(['script', 'style']):
                s.decompose()
            content_html = str(content_div)
        else:
            content_html = ""

        tags_span = soup.find('span', class_='tags-links')
        tags = [a.text.strip() for a in tags_span.find_all('a')] if tags_span else []

        # Find all image URLs within the article content
        image_urls = []
        if content_div:
            for img_tag in content_div.find_all('img'):
                if img_src := img_tag.get('src'):
                    full_img_url = urljoin(base_url, img_src)
                    image_urls.append(full_img_url)

        # Extract published date from article page
        published_date = ""
        date_element = soup.find('time', class_='entry-date') or soup.find('span', class_='published')
        if date_element:
            date_text = date_element.text.strip()
            # Try to parse various date formats
            try:
                # Common formats: "17 Oktober 2024", "October 17, 2024", etc.
                if ' ' in date_text:
                    parts = date_text.split()
                    if len(parts) >= 3:
                        day = parts[0]
                        month = parts[1]
                        year = parts[2]
                        # Convert Indonesian month names to numbers
                        month_map = {
                            'Januari': '01', 'Februari': '02', 'Maret': '03', 'April': '04', 'Mei': '05', 'Juni': '06',
                            'Juli': '07', 'Agustus': '08', 'September': '09', 'Oktober': '10', 'November': '11', 'Desember': '12',
                            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                            'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                        }
                        month_num = month_map.get(month, '01')
                        published_date = f"{year}-{month_num}-{day.zfill(2)}"
            except:
                pass

        category = determine_category(article_url, tags)

        return {
            "title": title,
            "content_html": content_html,
            "tags": tags,
            "image_urls": image_urls,
            "category": category,
            "published_date": published_date,
        }
    except Exception as e:
        print(f"  - Error scraping details from {article_url}: {e}")
        return None

def process_article(article_data, pg_conn):
    """
    Processes a single article: scrapes, downloads images, and prepares data for DB insertion.
    Returns a dictionary with all data ready for insertion, or None on failure.
    """
    post_id, article_url, thumbnail_url, published_date, base_url = article_data

    details = scrape_article_details(article_url, base_url)
    if not details:
        print(f"  - Failed to scrape details for {article_url}")
        return None

    # Skip articles with no title or content
    if not details.get('title') or details['title'] == "No Title Found":
        print(f"  - Skipping article with no title: {article_url}")
        return None
    if not details.get('content_html'):
        print(f"  - Skipping article with no content: {article_url}")
        return None

    # --- Image Processing ---
    # Download thumbnail
    thumbnail_local_path = download_image(thumbnail_url) if thumbnail_url else None
    thumbnail_web_path = f"{IMAGE_WEB_PATH}{os.path.basename(thumbnail_local_path)}" if thumbnail_local_path else None

    # Download content images and replace URLs in the content
    processed_content = details['content_html']
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(download_image, url): url for url in details['image_urls']}
        for future in as_completed(future_to_url):
            original_url = future_to_url[future]
            local_path = future.result()
            if local_path:
                web_path = f"{IMAGE_WEB_PATH}{os.path.basename(local_path)}"
                # Replace the original URL in the HTML with the new local web path
                processed_content = processed_content.replace(original_url, web_path)

    # --- Data Preparation ---
    slug = generate_slug(details['title'])
    # Use published date from article page if available, otherwise from list page
    article_date = details.get('published_date', '')
    parsed_date = parse_published_date(article_date) if article_date else parse_published_date(published_date)

    return {
        'post_id': post_id,
        'slug': slug,
        'title': details['title'],
        'subtitle': "", # Subtitle not scraped
        'thumbnail': thumbnail_web_path,
        'tags': json.dumps(details['tags']), # Store as JSON string
        'content': processed_content,
        'published_at': parsed_date,
        'author': "SMKN 2 Singosari", # Default author
        'category': details['category'],
        'url': article_url,
    }

def get_existing_post_ids(conn):
    """Get all existing post IDs from PostgreSQL to avoid duplicates."""
    with conn.cursor() as c:
        c.execute("SELECT slug FROM news")
        return set(row[0] for row in c.fetchall())

def generate_slug(title):
    """Generate a URL-friendly slug from a title."""
    slug = re.sub(r'[^\w\s-]', '', title).strip().lower()
    slug = re.sub(r'[\s-]+', '-', slug)
    return slug

def parse_published_date(date_str):
    """Parse various date string formats into a standard ISO format."""
    if date_str and date_str.strip():
        try:
            # Handle "YYYY-MM-DD" format
            if len(date_str) == 10 and date_str.count('-') == 2:
                return datetime.datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
            # Handle "YYYY-MM-DD" with placeholder year - use 2024
            elif 'YYYY-' in date_str:
                date_str = date_str.replace('YYYY', '2024')
                return datetime.datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
        except ValueError:
            pass # Fallback to now
    return datetime.datetime.now().date().isoformat()

def insert_batch(conn, batch):
    """Insert a batch of articles into the PostgreSQL database."""
    with conn.cursor() as c:
        for article in batch:
            # Using ON CONFLICT to prevent duplicates based on the unique slug
            c.execute(
                """
                INSERT INTO news (slug, title, subtitle, thumbnail, tags, content, published_at, author)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO NOTHING
                """,
                (
                    article['slug'], article['title'], article['subtitle'],
                    article['thumbnail'], article['tags'], article['content'],
                    article['published_at'], article['author']
                )
            )
    conn.commit()
    print(f"  -> Committed batch of {len(batch)} articles.")

def main():
    """Main function to run the scraper."""
    setup_directories()
    pg_conn = create_pg_connection()

    base_url = "https://smkn2-singosari.sch.id/"
    news_category_url = f"{base_url}?cat=4"

    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Mei': '05', 'Jun': '06',
        'Jul': '07', 'Agu': '08', 'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
    }

    # Get existing post slugs to avoid re-scraping
    existing_slugs = get_existing_post_ids(pg_conn)
    print(f"Found {len(existing_slugs)} existing articles in PostgreSQL.")

    all_tasks = []
    consecutive_empty_pages = 0
    
    for page in range(1, 200):  # Increased page limit
        url = f"{news_category_url}&paged={page}" if page > 1 else news_category_url
        print(f"\nScanning page {page}: {url}")

        try:
            list_response = SESSION.get(url, timeout=15)
            list_response.raise_for_status()
            list_soup = BeautifulSoup(list_response.content, 'lxml')
            
            # Find the primary content area
            primary_div = list_soup.find('div', id='primary')
            
            if not primary_div:
                print(f"  -> Warning: Could not find primary div on page {page}")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                    print(f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} consecutive pages without content.")
                    break
                time.sleep(1)  # Brief pause before next page
                continue
            
            # Find all article elements
            articles_on_page = primary_div.find_all('article', class_=re.compile(r'\bpost-\d+\b'))

            print(f"  -> Found {len(articles_on_page)} articles on page {page}.")

            if not articles_on_page:
                consecutive_empty_pages += 1
                print(f"  -> No articles found. Consecutive empty pages: {consecutive_empty_pages}")
                if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                    print(f"Stopping: {MAX_CONSECUTIVE_EMPTY_PAGES} consecutive pages without articles.")
                    break
                time.sleep(1)  # Brief pause before next page
                continue
            
            # Reset counter when we find articles
            consecutive_empty_pages = 0

            for article_summary in articles_on_page:
                try:
                    post_id_str = article_summary.get('id', '').replace('post-', '')
                    if not post_id_str.isdigit():
                        continue

                    post_id = int(post_id_str)

                    article_link_tag = article_summary.find('h2', class_='entry-title')
                    if not article_link_tag:
                        print(f"  -> Warning: No entry-title found for post {post_id}")
                        continue
                    
                    article_link = article_link_tag.find('a')
                    if not article_link or not article_link.get('href'):
                        print(f"  -> Warning: No link found for post {post_id}")
                        continue
                    
                    article_url = article_link['href']

                    thumbnail_tag = article_summary.find('img', class_='wp-post-image')
                    thumbnail_url = thumbnail_tag['src'] if thumbnail_tag else None

                    date_div = article_summary.find('div', class_='custom-entry-date')
                    if date_div:
                        month_span = date_div.find('span', class_='entry-month')
                        day_span = date_div.find('span', class_='entry-day')
                        
                        if month_span and day_span:
                            month_str = month_span.text.strip()
                            day_str = day_span.text.strip().zfill(2)
                            month_num = month_map.get(month_str, '01')
                            published_date = f"YYYY-{month_num}-{day_str}"
                        else:
                            published_date = ""
                    else:
                        published_date = ""

                    all_tasks.append((post_id, article_url, thumbnail_url, published_date, base_url))
                    
                except Exception as e:
                    print(f"  -> Error processing article on page {page}: {e}")
                    continue

            # Small delay between pages to be respectful to the server
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"  - Network error on page {page}: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                print(f"Stopping: Too many consecutive errors.")
                break
            time.sleep(2)  # Wait longer after an error
            continue
        except Exception as e:
            print(f"  - Unexpected error on page {page}: {e}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES:
                print(f"Stopping: Too many consecutive errors.")
                break
            continue

    print(f"\nFound {len(all_tasks)} total articles to check.")
    print("Processing articles in parallel...")

    processed_count = 0
    new_articles_batch = []

    for task in all_tasks:
        result = process_article(task, pg_conn)
        if result:
            # Final check to avoid duplicates before adding to batch
            if result['slug'] not in existing_slugs:
                new_articles_batch.append(result)
                existing_slugs.add(result['slug']) # Add to set to handle in-run duplicates
                processed_count += 1
                print(f"  - Queued for insertion ({processed_count}): {result['title'][:50]}...")
            
            if len(new_articles_batch) >= BATCH_SIZE:
                insert_batch(pg_conn, new_articles_batch)
                new_articles_batch = []

    # Insert any remaining articles in the last batch
    if new_articles_batch:
        insert_batch(pg_conn, new_articles_batch)

    pg_conn.close()
    print(f"\nScraping finished. Added {processed_count} new articles to the database.")

if __name__ == '__main__':
    main()