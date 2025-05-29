import xml.etree.ElementTree as ET
from typing import List, Set
from urllib.parse import urlparse

import duckdb
import pandas as pd
import requests
from pydantic import BaseModel


class FeedItem(BaseModel):
    title: str
    link: str
    guid: str
    categories: List[str]
    dc_creator: str
    pub_date: str
    atom_updated: str
    content_encoded: str


def clean_url(url: str) -> str:
    """Remove query parameters from URL to match against database."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def cdata_wrapper(text: str) -> str:
    """Wrap text in CDATA section."""
    return f"<![CDATA[ {text} ]]>"


def fetch_rss_feed(url: str) -> ET.Element:
    """Fetch and parse RSS feed from URL."""
    response = requests.get(url)
    response.raise_for_status()
    return ET.fromstring(response.content)


def get_existing_links(db_path: str) -> Set[str]:
    """Get all existing links from the database."""
    con = duckdb.connect(database=db_path, read_only=True)
    try:
        result = con.execute("SELECT DISTINCT link FROM medium_feed").fetchall()
        return {clean_url(row[0]) for row in result}
    except Exception:
        # Table might not exist yet
        return set()
    finally:
        con.close()


def parse_feed_item(item: ET.Element) -> FeedItem:
    """Parse an RSS feed item into a FeedItem object."""
    title = item.find("title").text or ""
    link = item.find("link").text or ""
    guid = item.find("guid").text or ""

    # Parse categories
    categories = []
    for category in item.findall("category"):
        if category.text:
            categories.append(category.text.strip())

    # Parse creator
    dc_creator = ""
    creator_elem = item.find(".//{http://purl.org/dc/elements/1.1/}creator")
    if creator_elem is not None:
        dc_creator = creator_elem.text or ""

    # Parse dates
    pub_date = item.find("pubDate").text or ""

    atom_updated = ""
    updated_elem = item.find(".//{http://www.w3.org/2005/Atom}updated")
    if updated_elem is not None:
        atom_updated = updated_elem.text or ""
    else:
        atom_updated = pub_date

    # Parse content
    content_encoded = ""
    content_elem = item.find(".//{http://purl.org/rss/1.0/modules/content/}encoded")
    if content_elem is not None:
        content_encoded = content_elem.text or ""

    # Also try description if content is empty
    if not content_encoded:
        desc_elem = item.find("description")
        if desc_elem is not None:
            content_encoded = desc_elem.text or ""

    return FeedItem(
        title=title.strip(),
        link=link.strip(),
        guid=guid.strip(),
        categories=categories,
        dc_creator=dc_creator.strip(),
        pub_date=pub_date.strip(),
        atom_updated=atom_updated.strip(),
        content_encoded=content_encoded.strip(),
    )


def get_new_items(feed_url: str, existing_links: Set[str]) -> List[FeedItem]:
    """Get new items from RSS feed that aren't in the database."""
    root = fetch_rss_feed(feed_url)
    new_items = []

    for item in root.findall(".//item"):
        link_elem = item.find("link")
        if link_elem is not None and link_elem.text:
            clean_link = clean_url(link_elem.text)
            if clean_link not in existing_links:
                try:
                    feed_item = parse_feed_item(item)
                    new_items.append(feed_item)
                except Exception as e:
                    print(f"Error parsing item {clean_link}: {e}")

    return new_items


def add_items_to_database(items: List[FeedItem], db_path: str):
    """Add new items to the database."""
    if not items:
        return

    con = duckdb.connect(database=db_path, read_only=False)
    try:
        # Create table if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS medium_feed (
                title VARCHAR,
                link VARCHAR,
                guid VARCHAR,
                categories VARCHAR,
                dc_creator VARCHAR,
                pub_date VARCHAR,
                atom_updated VARCHAR,
                content_encoded VARCHAR
            )
        """)

        # Convert items to dataframe
        df = pd.DataFrame([item.model_dump() for item in items])
        # Convert categories list to string representation for database storage
        df["categories"] = df["categories"].apply(
            lambda x: str(x) if isinstance(x, list) else x
        )

        # Insert new items
        con.execute("INSERT INTO medium_feed SELECT * FROM df")
        print(f"Added {len(items)} new items to database")

    finally:
        con.close()


def update_xml_feed(new_items: List[FeedItem], xml_path: str):
    """Add new items to the top of the existing XML feed."""
    if not new_items:
        return

    # Read existing feed as text and use string manipulation for safety
    try:
        with open(xml_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Generate new item XML strings using the same format as the backfill
        feed_frame = """<rss xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:cc="http://cyber.law.harvard.edu/rss/creativeCommonsRssModule.html" version="2.0">
<channel>
<title>
<![CDATA[ Generative AI in the Newsroom - Medium ]]>
</title>
<description>
<![CDATA[ The Generative AI in the Newsroom project is an effort to collaboratively figure out how and when (or when not) to use generative AI in news production. ]]>
</description>
<link>https://github.com/NHagar/gain-rss/blob/main/data/gain_feed.xml</link>
<image>
<url>https://cdn-images-1.medium.com/proxy/1*TGH72Nnw24QL3iV9IOm4VA.png</url>
<title>Generative AI in the Newsroom - Medium</title>
<link>https://github.com/NHagar/gain-rss/blob/main/data/gain_feed.xml</link>
</image>
<generator>GAIN GH Action</generator>
<lastBuildDate>Thu, 29 May 2025 18:16:28 GMT</lastBuildDate>
<atom:link href="" rel="self" type="application/rss+xml"/>
<webMaster>
<![CDATA[ nicholas.hagar@northwestern.edu ]]>
</webMaster>
{items}
</channel>
</rss>
"""

        item_frame = """<item>
<title>{title}</title>
<link>{link}</link>
<guid isPermaLink="false">{guid}</guid>
{categories}
<dc:creator>{dc_creator}</dc:creator>
<pubDate>{pub_date}</pubDate>
<atom:updated>{atom_updated}</atom_updated>
<content:encoded>{content_encoded}</content:encoded>
</item>"""

        categories_frame = """<category>{cat}</category>"""

        def make_item(feed_item: FeedItem) -> str:
            # Clean and prepare the data with CDATA wrapping
            title = cdata_wrapper(
                feed_item.title.replace("<![CDATA[", "").replace("]]>", "")
            )
            dc_creator = cdata_wrapper(
                feed_item.dc_creator.replace("<![CDATA[", "").replace("]]>", "")
            )
            content_encoded = cdata_wrapper(
                feed_item.content_encoded.replace("<![CDATA[", "").replace("]]>", "")
            )

            # Handle categories - clean them and wrap in CDATA
            categories = ""
            for cat in feed_item.categories:
                clean_cat = cat.replace("<![CDATA[", "").replace("]]>", "")
                categories += categories_frame.format(cat=cdata_wrapper(clean_cat))

            return item_frame.format(
                title=title,
                link=feed_item.link,
                guid=feed_item.guid,
                categories=categories,
                dc_creator=dc_creator,
                pub_date=feed_item.pub_date,
                atom_updated=feed_item.atom_updated,
                content_encoded=content_encoded,
            )

        # Extract existing items from current feed
        first_item_start = content.find("<item>")
        if first_item_start == -1:
            # No existing items, just add the new ones
            existing_items = ""
        else:
            last_item_end = content.rfind("</item>") + 7  # +7 for '</item>'
            existing_items = content[first_item_start:last_item_end]

        # Generate new items
        new_items_xml = "".join([make_item(item) for item in new_items])

        # Combine new and existing items
        all_items = new_items_xml + existing_items

        # Generate complete feed
        complete_feed = feed_frame.format(items=all_items)

        # Write back to file
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(complete_feed)

        print(f"Updated XML feed with {len(new_items)} new items")

    except FileNotFoundError:
        print(f"XML feed file not found at {xml_path}")
    except Exception as e:
        print(f"Error updating XML feed: {e}")


def main():
    """Main function to update the RSS feed."""
    rss_url = "https://generative-ai-newsroom.com/feed"
    db_path = "./data/gain_feed.duckdb"
    xml_path = "./data/gain_feed.xml"

    print("Starting RSS feed update...")

    try:
        # Ensure data directory exists
        import os

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Get existing links from database
        existing_links = get_existing_links(db_path)
        print(f"Found {len(existing_links)} existing links in database")

        # Get new items from RSS feed
        new_items = get_new_items(rss_url, existing_links)
        print(f"Found {len(new_items)} new items")

        if new_items:
            # Add new items to database
            add_items_to_database(new_items, db_path)

            # Update XML feed
            update_xml_feed(new_items, xml_path)

            print("Feed update completed successfully!")

            # Print summary of new items
            print("\nNew items added:")
            for item in new_items:
                print(f"- {item.title}")

        else:
            print("No new items found. Feed is up to date.")

    except requests.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        raise
    except Exception as e:
        print(f"Error updating feed: {e}")
        raise


if __name__ == "__main__":
    main()
