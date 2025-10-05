import sqlite3
import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin

DATABASE_NAME = 'news.db'
IMAGE_DIRECTORY = 'article_images'

def setup_database():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER UNIQUE,
            title TEXT,
            url TEXT,
            content TEXT,
            published_date TEXT,
            tags TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            image_path TEXT,
            is_thumbnail INTEGER,
            FOREIGN KEY(article_id) REFERENCES articles(id)
        )
    ''')
    conn.commit()
    conn.close()
    if not os.path.exists(IMAGE_DIRECTORY):
        os.makedirs(IMAGE_DIRECTORY)

def download_image(image_url, folder_path):
    if not image_url:
        return None
    try:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        response = requests.get(image_url, stream=True)
        response.raise_for_status()

        filename = os.path.join(folder_path, image_url.split('/')[-1])

        with open(filename, 'wb') as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        print(f"    - Image downloaded: {filename}")
        return filename
    except requests.exceptions.RequestException as e:
        print(f"    - Error downloading image {image_url}: {e}")
        return None

def scrape_article_details(article_url, base_url):
    try:
        response = requests.get(article_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        title_tag = soup.find('h1', class_='entry-title')
        title = title_tag.text.strip() if title_tag else "No Title Found"

        content_div = soup.find('div', class_='entry-content')
        content = content_div.get_text(separator='\n', strip=True) if content_div else ""

        tags_span = soup.find('span', class_='tags-links')
        if tags_span:
            tags = [a.text for a in tags_span.find_all('a')]
            tags_str = ', '.join(tags)
            year = next((tag for tag in tags if tag.isdigit() and len(tag) == 4), None)
        else:
            tags_str = ""
            year = None

        image_urls = []
        if content_div:
            for img_tag in content_div.find_all('img'):
                img_src = img_tag.get('src')
                if img_src:
                    # Ensure the URL is absolute
                    full_img_url = urljoin(base_url, img_src)
                    image_urls.append(full_img_url)

        return {
            "title": title,
            "content": content,
            "tags": tags_str,
            "year": year,
            "image_urls": image_urls
        }
    except Exception as e:
        print(f"  - Error scraping details from {article_url}: {e}")
        return None


def main():
    setup_database()
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()

    base_url = "https://smkn2-singosari.sch.id/"
    news_category_url = f"{base_url}?cat=4"

    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Mei': '05', 'Jun': '06',
        'Jul': '07', 'Agu': '08', 'Sep': '09', 'Okt': '10', 'Nov': '11', 'Des': '12'
    }

    for page in range(1, 23):
        url = f"{news_category_url}&paged={page}" if page > 1 else news_category_url
        print(f"\nScraping page {page}: {url}")

        try:
            list_response = requests.get(url)
            list_soup = BeautifulSoup(list_response.content, 'html.parser')
            articles_on_page = list_soup.find('div', id='primary').find_all('article', class_=re.compile(r'\bpost-\d+\b'))

            for article_summary in articles_on_page:
                post_id_str = article_summary.get('id', '').replace('post-', '')
                if not post_id_str.isdigit():
                    continue
                post_id = int(post_id_str)
                
                c.execute("SELECT id FROM articles WHERE post_id = ?", (post_id,))
                if c.fetchone():
                    print(f"  - Skipping article {post_id} (already in database).")
                    continue

                article_link_tag = article_summary.find('h2', class_='entry-title').find('a')
                article_url = article_link_tag['href']

                print(f"  - Processing article: {article_url}")

                details = scrape_article_details(article_url, base_url)
                if not details:
                    continue

                thumbnail_tag = article_summary.find('img', class_='wp-post-image')
                thumbnail_url = thumbnail_tag['src'] if thumbnail_tag else None

                date_div = article_summary.find('div', class_='custom-entry-date')
                if date_div:
                    month_str = date_div.find('span', class_='entry-month').text.strip()
                    day_str = date_div.find('span', class_='entry-day').text.strip().zfill(2)
                    month_num = month_map.get(month_str, '00')
                    year_str = details.get('year', 'YYYY')
                    published_date = f"{year_str}-{month_num}-{day_str}"
                else:
                    published_date = " "

                article_image_folder = os.path.join(IMAGE_DIRECTORY, str(post_id))
                thumbnail_path = download_image(thumbnail_url, article_image_folder)
                
                content_image_paths = []
                for img_url in details['image_urls']:
                    path = download_image(img_url, article_image_folder)
                    if path:
                        content_image_paths.append(path)

                c.execute(
                    "INSERT INTO articles (post_id, title, url, content, published_date, tags) VALUES (?, ?, ?, ?, ?, ?)",
                    (post_id, details['title'], article_url, details['content'], published_date, details['tags'])
                )
                article_db_id = c.lastrowid 

                if thumbnail_path:
                    c.execute(
                        "INSERT INTO images (article_id, image_path, is_thumbnail) VALUES (?, ?, ?)",
                        (article_db_id, thumbnail_path, 1)
                    )

                for img_path in content_image_paths:
                    c.execute(
                        "INSERT INTO images (article_id, image_path, is_thumbnail) VALUES (?, ?, ?)",
                        (article_db_id, img_path, 0)
                    )

                conn.commit()
                print(f"  - Successfully saved article '{details['title']}' to database.")

        except Exception as e:
            print(f"  - Critical error on page {page}: {e}")

    conn.close()
    print("\nScraping finished.")

if __name__ == '__main__':
    main()