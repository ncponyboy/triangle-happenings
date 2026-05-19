"""
Triangle Happenings — Event Scraper
=====================================
Scrapes multiple Triangle-area (Raleigh · Durham · Chapel Hill) event sites
and writes triangle_happenings.json to the repo root for the iOS app to consume.

Runs daily via GitHub Actions.

Required environment variables (GitHub Secrets):
  GEEKFLARE_API_KEY  — headless-Chrome scraping API (geekflare.com)
  NPS_API_KEY        — NPS developer API key (free at nps.gov/subjects/developer)
"""

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup
from html import unescape as html_unescape

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
GEEKFLARE_API_URL = "https://api.geekflare.com/webscraping"
GEEKFLARE_API_KEY = os.environ.get("GEEKFLARE_API_KEY", "")
NPS_API_KEY       = os.environ.get("NPS_API_KEY", "")

OUTPUT_FILE       = os.path.join(os.path.dirname(__file__), "..", "triangle_happenings.json")
MANUAL_EVENTS_FILE = os.path.join(os.path.dirname(__file__), "..", "manual_events.json")


def log_info(msg):   print(f"[INFO]  {msg}")
def log_warn(msg):   print(f"[WARN]  {msg}")
def log_error(msg):  print(f"[ERROR] {msg}")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def clean_text(text: str) -> str:
    text = re.sub(r'<[^>]+>', '', text)
    text = html_unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def create_event_id(title: str, date: str, source: str) -> str:
    return hashlib.md5(f"{title}_{date}_{source}".encode()).hexdigest()[:12]


_MONTH_ABBR = {
    'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April',
    'may': 'May', 'jun': 'June', 'jul': 'July', 'aug': 'August',
    'sep': 'September', 'oct': 'October', 'nov': 'November', 'dec': 'December',
}


def parse_date_time(month: str, day: str, year: int = None, time_str: str = "7pm") -> Optional[datetime]:
    try:
        month = _MONTH_ABBR.get(month[:3].lower(), month)
        if year is None:
            year = datetime.now().year
        event_date = datetime.strptime(f"{month} {day} {year}", "%B %d %Y")
        if event_date < datetime.now() - timedelta(days=30):
            event_date = datetime.strptime(f"{month} {day} {year + 1}", "%B %d %Y")
        hour, minute = 19, 0
        time_lower = time_str.lower().strip()
        if '-' in time_lower:
            time_lower = time_lower.split('-')[0].strip()
        time_match = re.search(r'(\d+)(?::(\d+))?\s*(am|pm)?', time_lower)
        if time_match:
            hour = int(time_match.group(1))
            if time_match.group(2):
                minute = int(time_match.group(2))
            am_pm = time_match.group(3)
            if am_pm == 'pm' and hour < 12:
                hour += 12
            elif am_pm == 'am' and hour == 12:
                hour = 0
            elif not am_pm and hour < 12:
                hour += 12
        event_date = event_date.replace(hour=hour, minute=minute)
        if event_date > datetime.now() + timedelta(days=365):
            return None
        return event_date
    except Exception as e:
        log_error(f"Error parsing date '{month} {day} {time_str}': {e}")
        return None


def deduplicate_events(events: List[Dict]) -> List[Dict]:
    if not events:
        return []
    unique_events = []
    for event in events:
        is_duplicate = False
        for existing in unique_events:
            if event['date'][:10] != existing['date'][:10]:
                continue
            et = event['title'].lower().strip()
            ext = existing['title'].lower().strip()
            if et == ext or et in ext or ext in et:
                is_duplicate = True
                if len(event['title']) > len(existing['title']):
                    existing['title'] = event['title']
                    existing['description'] = event.get('description', '')
                break
        if not is_duplicate:
            unique_events.append(event)
    return unique_events


async def fetch_url(url: str, session: aiohttp.ClientSession, extra_headers: dict = None) -> Optional[str]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(2):
        try:
            response = await session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20))
            if response.status == 200:
                text = await response.text()
                await response.release()
                return text
            log_warn(f"Got status {response.status} for {url}")
            await response.release()
        except Exception as e:
            if attempt == 1:
                log_error(f"Error fetching {url}: {e}")
            else:
                log_info(f"Retry for {url}")
    return None


# ─────────────────────────────────────────────
# Geekflare Web Scraping API helper
# ─────────────────────────────────────────────
async def fetch_with_geekflare(url: str, session: aiohttp.ClientSession) -> Optional[str]:
    if not GEEKFLARE_API_KEY:
        log_warn("GEEKFLARE_API_KEY not set — skipping JS-rendered scrape")
        return None
    payload = {"url": url, "format": "html"}
    headers = {"x-api-key": GEEKFLARE_API_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(
            GEEKFLARE_API_URL, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=45)
        ) as response:
            if response.status == 401:
                log_error("Geekflare: invalid API key (401)")
                return None
            if response.status == 402:
                log_error("Geekflare: out of credits (402)")
                return None
            if response.status not in (200, 201):
                log_error(f"Geekflare returned {response.status} for {url}")
                return None
            raw = await response.text()
            html = ""
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    html = (
                        data.get("data", {}).get("content", "")
                        or data.get("data", {}).get("html", "")
                        or data.get("result", {}).get("content", "")
                        or data.get("result", {}).get("html", "")
                        or data.get("content", "")
                        or data.get("html", "")
                        or ""
                    )
                    if not html:
                        log_error(f"Geekflare JSON but no html field. Keys: {list(data.keys())}")
                        return None
                else:
                    log_error(f"Geekflare JSON but not a dict: {raw[:200]}")
                    return None
            except Exception:
                if raw.strip().startswith("<") or "<html" in raw.lower():
                    html = raw
                else:
                    log_error(f"Geekflare response not JSON or HTML: {raw[:300]}")
                    return None
            log_info(f"Geekflare fetched {len(html)} chars from {url}")
            return html
    except Exception as e:
        log_error(f"Geekflare fetch error for {url}: {e}")
        return None


# ─────────────────────────────────────────────
# iCal Parser Helper
# ─────────────────────────────────────────────
def parse_ical_feed(raw_ical: str, source_name: str, default_location: str,
                    default_lat: float, default_lon: float, source_url: str) -> List[Dict]:
    events = []
    seen = set()
    cutoff = datetime.now() - timedelta(hours=2)
    unfolded = re.sub(r'\r?\n[ \t]', '', raw_ical)
    vevent_re = re.compile(r'BEGIN:VEVENT(.*?)END:VEVENT', re.DOTALL)
    field_re  = re.compile(r'^([A-Z][A-Z0-9-]*(?:;[^:]+)?):(.*)', re.MULTILINE)
    for vevent_match in vevent_re.finditer(unfolded):
        block = vevent_match.group(1)
        fields = {}
        for fm in field_re.finditer(block):
            key_full = fm.group(1)
            val = fm.group(2).strip()
            key = key_full.split(';')[0].upper()
            if key not in fields:
                fields[key] = val
        summary  = fields.get('SUMMARY', '').replace('\\n', ' ').replace('\\,', ',').replace('\\;', ';').strip()
        dtstart  = fields.get('DTSTART', '').strip()
        location = fields.get('LOCATION', default_location).replace('\\n', ', ').replace('\\,', ',').strip() or default_location
        description = fields.get('DESCRIPTION', '').replace('\\n', ' ').replace('\\,', ',').replace('\\;', ';').strip()
        url_field   = fields.get('URL', source_url).strip()
        if not summary or not dtstart:
            continue
        dtstart_clean = dtstart.upper().rstrip('Z')
        try:
            if 'T' in dtstart_clean:
                dt_part, t_part = dtstart_clean.split('T', 1)
                t_part = t_part[:6].ljust(6, '0')
                event_date = datetime.strptime(dt_part + t_part, '%Y%m%d%H%M%S')
            else:
                event_date = datetime.strptime(dtstart_clean[:8], '%Y%m%d')
                event_date = event_date.replace(hour=19, minute=0)
        except Exception:
            continue
        if event_date < cutoff:
            continue
        title = clean_text(summary)
        if not title:
            continue
        key = f"{title.lower()}_{event_date.date()}"
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "id": create_event_id(title, event_date.isoformat(), source_name),
            "title": title,
            "date": event_date.isoformat(),
            "location": clean_text(location)[:100],
            "description": clean_text(description)[:200] if description else '',
            "source": source_name,
            "url": url_field or source_url,
            "latitude": default_lat,
            "longitude": default_lon,
        })
    return events


def _parse_tribe_events(soup, source_name: str, source_url: str,
                        default_location: str, lat: float, lon: float,
                        base_url: str = "") -> List[Dict]:
    """Generic parser for The Events Calendar (tribe) powered sites."""
    events = []
    seen = set()
    cutoff = datetime.now() - timedelta(hours=2)
    date_re = re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2})(?:,?\s+(\d{4}))?', re.IGNORECASE
    )
    time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
    skip_titles = {'events', 'upcoming events', 'all events', 'list', 'month', 'week', 'day', 'today', 'more events'}

    blocks = (
        soup.find_all('article', class_=lambda c: c and 'tribe' in (' '.join(c) if isinstance(c, list) else c))
        or soup.find_all('li', class_=lambda c: c and 'tribe' in (' '.join(c) if isinstance(c, list) else c))
        or soup.find_all('div', class_=lambda c: c and 'tribe' in (' '.join(c) if isinstance(c, list) else c))
        or soup.find_all(['article'], class_=lambda c: c and 'event' in (' '.join(c) if isinstance(c, list) else c))
        or soup.find_all(['h3', 'h4'])
    )
    for element in blocks:
        try:
            heading = element.find(['h2', 'h3', 'h4']) if element.name not in ('h3', 'h4') else element
            if not heading:
                continue
            link = heading.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
                continue
            event_url = link.get('href', source_url)
            if event_url and event_url.startswith('/'):
                event_url = base_url + event_url
            block_text = element.get_text() if element.name not in ('h3', 'h4') else (heading.parent.get_text() if heading.parent else heading.get_text())
            dm = date_re.search(block_text)
            if not dm:
                continue
            month = dm.group(1).title()
            day = dm.group(2)
            year = int(dm.group(3)) if dm.group(3) else datetime.now().year
            tm = time_re.search(block_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 pm")
            if not event_date or event_date < cutoff:
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            loc_el = element.find(class_=lambda c: c and ('venue' in c.lower() or 'location' in c.lower()))
            location = loc_el.get_text(strip=True) if loc_el else default_location
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": location or default_location,
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": lat,
                "longitude": lon,
            })
        except Exception as e:
            log_error(f"  ✗ {source_name} item parse error: {e}")
    return events


def _parse_seated_events(html: str, source_name: str, default_location: str,
                         lat: float, lon: float, base_url: str) -> List[Dict]:
    """Parse music venues using Seated-style ticketing platform (href=/event/...)."""
    soup = BeautifulSoup(html, 'html.parser')
    events = []
    seen = set()
    cutoff = datetime.now() - timedelta(hours=2)
    short_date_re = re.compile(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+'
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:,?\s+(\d{4}))?',
        re.IGNORECASE
    )
    time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
    month_map = {
        'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April',
        'may': 'May', 'jun': 'June', 'jul': 'July', 'aug': 'August',
        'sep': 'September', 'oct': 'October', 'nov': 'November', 'dec': 'December',
    }
    skip_titles = {'tickets', 'buy tickets', 'more info', 'sold out', 'events', 'shows', 'all shows'}
    for a in soup.find_all('a', href=re.compile(r'/event/')):
        href = a.get('href', '')
        if not href:
            continue
        event_url = href if href.startswith('http') else base_url.rstrip('/') + href
        # Prefer title attribute (RHP Events plugin stores event name there)
        title_attr = clean_text(a.get('title', ''))
        heading = a.find(['h1', 'h2', 'h3', 'h4', 'h5'])
        title_text = (
            title_attr
            or clean_text(heading.get_text() if heading else '')
            or clean_text(a.get_text())
        )
        if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
            continue
        block_text = ''
        node = a
        for _ in range(5):
            node = node.parent
            if not node:
                break
            block_text = node.get_text()
            if short_date_re.search(block_text):
                break
        dm = short_date_re.search(block_text)
        if not dm:
            continue
        month = month_map.get(dm.group(1)[:3].lower(), dm.group(1).title())
        day = dm.group(2)
        year = int(dm.group(3)) if dm.group(3) else datetime.now().year
        tm = time_re.search(block_text)
        event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "8:00 pm")
        if not event_date or event_date < cutoff:
            continue
        key = f"{title_text.lower()}_{event_date.date()}"
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "id": create_event_id(title_text, event_date.isoformat(), source_name),
            "title": title_text,
            "date": event_date.isoformat(),
            "location": default_location,
            "description": "",
            "source": source_name,
            "url": event_url,
            "latitude": lat,
            "longitude": lon,
        })
    return events


def _parse_show_run_events(html: str, source_name: str, default_location: str,
                           lat: float, lon: float, base_url: str,
                           show_url_pattern: str = r'/show') -> List[Dict]:
    """Parse theatre sites that list season shows with date ranges like 'Jun 5 - 28, 2026'."""
    soup = BeautifulSoup(html, 'html.parser')
    events = []
    seen = set()
    cutoff = datetime.now() - timedelta(hours=2)
    # Matches "Sep 9-27, 2026", "June 5 - 28, 2026", "Sep 9 - Oct 5, 2026"
    _M = r'(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    range_re = re.compile(
        rf'({_M})\s+(\d{{1,2}})\s*[-–]\s*(?:({_M})\s+)?(\d{{1,2}}),?\s*(\d{{4}})',
        re.IGNORECASE
    )
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        link = heading.find('a', href=re.compile(show_url_pattern))
        if not link:
            continue
        title_text = clean_text(link.get_text())
        if not title_text or len(title_text) < 4:
            continue
        event_url = link.get('href', '')
        if event_url.startswith('/'):
            event_url = base_url.rstrip('/') + event_url
        # Search surrounding context for date range
        block = heading.parent or heading
        block_text = block.get_text()
        dm = range_re.search(block_text)
        if not dm:
            continue
        month = dm.group(1).title()
        day = dm.group(2)
        year = int(dm.group(5))
        event_date = parse_date_time(month, day, year=year, time_str="7:30 pm")
        if not event_date or event_date < cutoff:
            # If opening night has passed, use the whole run as a single event at run start
            continue
        key = f"{title_text.lower()}_{event_date.date()}"
        if key in seen:
            continue
        seen.add(key)
        # Build readable date range string for description
        # Skip if parse_date_time rolled the year (opening night was in the past)
        if event_date.year != year:
            continue
        end_month = dm.group(3).title() if dm.group(3) else month
        end_day = dm.group(4)
        desc = f"Running {month} {day} – {end_month} {end_day}, {year}"
        events.append({
            "id": create_event_id(title_text, event_date.isoformat(), source_name),
            "title": title_text,
            "date": event_date.isoformat(),
            "location": default_location,
            "description": desc,
            "source": source_name,
            "url": event_url,
            "latitude": lat,
            "longitude": lon,
        })
    return events


# ─────────────────────────────────────────────
# Scrapers — Triangle area
# ─────────────────────────────────────────────

async def scrape_visit_raleigh(session: aiohttp.ClientSession) -> List[Dict]:
    """visitraleigh.com — official Raleigh CVB events calendar."""
    log_info("Scraping Visit Raleigh...")
    events = []
    source_name = "Visit Raleigh"
    base_url = "https://www.visitraleigh.com"
    ical_url = f"{base_url}/events/?ical=1"
    try:
        raw = await fetch_url(ical_url, session)
        if raw and 'BEGIN:VCALENDAR' in raw:
            events = parse_ical_feed(raw, source_name, "Raleigh, NC", 35.7796, -78.6382, f"{base_url}/events/")
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        # Fallback: scrape HTML events listing
        html = await fetch_url(f"{base_url}/events/", session) or await fetch_with_geekflare(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", "Raleigh, NC", 35.7796, -78.6382, base_url)
        if not events:
            # JSON-LD fallback
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        start_date = item.get('startDate', '')
                        if not title_text or not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < datetime.now() - timedelta(hours=2):
                            continue
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": "Raleigh, NC",
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', f"{base_url}/events/"),
                            "latitude": 35.7796,
                            "longitude": -78.6382,
                        })
                except Exception:
                    continue
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Visit Raleigh error: {e}")
    return events


async def scrape_indy_week(session: aiohttp.ClientSession) -> List[Dict]:
    """indyweek.com — Triangle's independent arts & culture weekly."""
    log_info("Scraping Indy Week calendar...")
    events = []
    source_name = "Indy Week"
    base_url = "https://indyweek.com"
    urls = [
        f"{base_url}/events/calendar/",
        f"{base_url}/calendar/",
        f"{base_url}/events/",
    ]
    try:
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\s+(\d{1,2})(?:,?\s+(\d{4}))?', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        skip_titles = {'calendar', 'events', 'all events', 'upcoming', 'arts', 'music', 'food', 'nightlife', 'more'}
        for url in urls:
            html = await fetch_url(url, session)
            if not html or len(html) < 1000:
                continue
            # Try JSON-LD first
            soup = BeautifulSoup(html, 'html.parser')
            found_jsonld = []
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        start_date = item.get('startDate', '')
                        if not title_text or not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < cutoff:
                            continue
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        loc_obj = item.get('location', {})
                        location = "Raleigh-Durham area, NC"
                        if isinstance(loc_obj, dict):
                            venue = clean_text(loc_obj.get('name', ''))
                            addr = loc_obj.get('address', {})
                            if isinstance(addr, dict):
                                city = addr.get('addressLocality', '')
                                if city:
                                    location = f"{venue}, {city}" if venue else city
                            elif venue:
                                location = venue
                        found_jsonld.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', url),
                            "latitude": 35.9940,
                            "longitude": -78.8986,
                        })
                except Exception:
                    continue
            if found_jsonld:
                events.extend(found_jsonld)
                log_info(f"  ✓ Found {len(found_jsonld)} events via JSON-LD")
                break
            # HTML fallback
            for heading in soup.find_all(['h2', 'h3', 'h4']):
                link = heading.find('a')
                if not link:
                    continue
                title_text = clean_text(link.get_text())
                if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
                    continue
                event_url = link.get('href', url)
                if event_url.startswith('/'):
                    event_url = base_url + event_url
                container = heading.parent or heading
                block_text = container.get_text()
                dm = date_re.search(block_text)
                if not dm:
                    continue
                month = dm.group(1).title()
                day = dm.group(2)
                year = int(dm.group(3)) if dm.group(3) else datetime.now().year
                tm = time_re.search(block_text)
                event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 pm")
                if not event_date or event_date < cutoff:
                    continue
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": "Raleigh-Durham area, NC",
                    "description": "",
                    "source": source_name,
                    "url": event_url,
                    "latitude": 35.9940,
                    "longitude": -78.8986,
                })
            if events:
                log_info(f"  ✓ Found {len(events)} events via HTML")
                break
        if not events:
            log_warn("  ⚠ Indy Week: no events found")
    except Exception as e:
        log_error(f"  ✗ Indy Week error: {e}")
    return events


async def scrape_visit_durham(session: aiohttp.ClientSession) -> List[Dict]:
    """discoverdurham.com — Durham tourism events (visitdurham.com redirects here)."""
    log_info("Scraping Discover Durham...")
    events = []
    source_name = "Visit Durham"
    base_url = "https://www.discoverdurham.com"
    try:
        # Try iCal
        for ical_path in ["/events/?ical=1", "/events/ical/"]:
            ical_raw = await fetch_url(f"{base_url}{ical_path}", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(ical_raw, source_name, "Durham, NC", 35.9940, -78.8986, f"{base_url}/events/")
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
        html = await fetch_url(f"{base_url}/events/", session) or await fetch_with_geekflare(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        # JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if 'Event' not in item.get('@type', ''):
                        continue
                    title_text = clean_text(item.get('name', ''))
                    start_date = item.get('startDate', '')
                    if not title_text or not start_date:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                        event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": "Durham, NC",
                        "description": clean_text(item.get('description', ''))[:200],
                        "source": source_name,
                        "url": item.get('url', f"{base_url}/events/"),
                        "latitude": 35.9940,
                        "longitude": -78.8986,
                    })
            except Exception:
                continue
        if not events:
            # Full month name date regex fallback for CVB-style HTML
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", "Durham, NC", 35.9940, -78.8986, base_url)
        if not events:
            # discoverdurham.com uses concatenated dates like "May192026" as link text
            concat_date_re = re.compile(
                r'^(January|February|March|April|May|June|July|August|September|October|November|December)'
                r'(\d{1,2})(\d{4})$',
                re.IGNORECASE
            )
            for a_tag in soup.find_all('a', href=re.compile(r'/events/')):
                date_text = clean_text(a_tag.get_text())
                dm = concat_date_re.match(date_text)
                if not dm:
                    continue
                month = dm.group(1).title()
                day = dm.group(2)
                year = int(dm.group(3))
                event_date = parse_date_time(month, day, year=year, time_str="12:00 pm")
                if not event_date or event_date < cutoff:
                    continue
                # Title is usually in a heading sibling or the href itself
                card = a_tag.parent
                title_tag = card.find(['h2', 'h3', 'h4']) if card else None
                if not title_tag:
                    card = card.parent if card else None
                    title_tag = card.find(['h2', 'h3', 'h4']) if card else None
                if not title_tag:
                    continue
                title_link = title_tag.find('a') or title_tag
                title_text = clean_text(title_link.get_text())
                if not title_text or len(title_text) < 4:
                    continue
                event_url = title_link.get('href', f"{base_url}/events/") if hasattr(title_link, 'get') else f"{base_url}/events/"
                if event_url.startswith('/'):
                    event_url = base_url + event_url
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": "Durham, NC",
                    "description": "",
                    "source": source_name,
                    "url": event_url,
                    "latitude": 35.9940,
                    "longitude": -78.8986,
                })
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Visit Durham error: {e}")
    return events


async def scrape_chapelboro(session: aiohttp.ClientSession) -> List[Dict]:
    """chapelboro.com — Chapel Hill/Carrboro community events (WCHL)."""
    log_info("Scraping Chapelboro events...")
    events = []
    source_name = "Chapelboro"
    base_url = "https://chapelboro.com"
    try:
        # Try iCal
        ical_raw = await fetch_url(f"{base_url}/events/?ical=1", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(ical_raw, source_name, "Chapel Hill, NC", 35.9132, -79.0558, f"{base_url}/events/")
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        # Try RSS
        for rss_path in ["/events/feed/", "/feed/?post_type=tribe_events", "/events/?feed=rss2"]:
            rss_raw = await fetch_url(f"{base_url}{rss_path}", session)
            if rss_raw and '<item>' in rss_raw:
                cutoff = datetime.now() - timedelta(hours=2)
                seen = set()
                title_re = re.compile(r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>', re.DOTALL)
                link_re  = re.compile(r'<link>(.*?)</link>', re.DOTALL)
                date_re2 = re.compile(r'<pubDate>(.*?)</pubDate>|<startDate>(.*?)</startDate>', re.DOTALL)
                items = re.split(r'<item>', rss_raw)[1:]
                for item in items:
                    tm = title_re.search(item)
                    lm = link_re.search(item)
                    dm = date_re2.search(item)
                    if not tm or not dm:
                        continue
                    title_text = clean_text(tm.group(1) or tm.group(2) or "")
                    link_url = (lm.group(1) or "").strip() if lm else f"{base_url}/events/"
                    date_str = (dm.group(1) or dm.group(2) or "").strip()
                    try:
                        from email.utils import parsedate_to_datetime
                        event_date = parsedate_to_datetime(date_str).replace(tzinfo=None)
                    except Exception:
                        continue
                    if not title_text or event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": "Chapel Hill, NC",
                        "description": "",
                        "source": source_name,
                        "url": link_url,
                        "latitude": 35.9132,
                        "longitude": -79.0558,
                    })
                if events:
                    log_info(f"  ✓ Found {len(events)} events via RSS")
                    return events
        # HTML fallback
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        # Try JSON-LD first
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if 'Event' not in item.get('@type', ''):
                        continue
                    title_text = clean_text(item.get('name', ''))
                    start_date = item.get('startDate', '')
                    if not title_text or not start_date:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                        event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": "Chapel Hill, NC",
                        "description": clean_text(item.get('description', ''))[:200],
                        "source": source_name,
                        "url": item.get('url', f"{base_url}/events/"),
                        "latitude": 35.9132,
                        "longitude": -79.0558,
                    })
            except Exception:
                continue
        if not events:
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", "Chapel Hill, NC", 35.9132, -79.0558, base_url)
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Chapelboro error: {e}")
    return events


async def scrape_dpac(session: aiohttp.ClientSession) -> List[Dict]:
    """dpacnc.com — Durham Performing Arts Center."""
    log_info("Scraping DPAC (Durham PAC)...")
    events = []
    source_name = "DPAC"
    base_url = "https://www.dpacnc.com"
    try:
        ical_raw = await fetch_url(f"{base_url}/events/?ical=1", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(
                ical_raw, source_name,
                "DPAC, 123 Vivian St, Durham, NC",
                35.9942, -78.9030, f"{base_url}/events/"
            )
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/events/", session) or await fetch_with_geekflare(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        cutoff = datetime.now() - timedelta(hours=2)
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\s+(\d{1,2})(?:,?\s+(\d{4}))?', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        # JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if 'Event' not in item.get('@type', ''):
                        continue
                    title_text = clean_text(item.get('name', ''))
                    start_date = item.get('startDate', '')
                    if not title_text or not start_date:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                        event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": "DPAC, 123 Vivian St, Durham, NC",
                        "description": clean_text(item.get('description', ''))[:200],
                        "source": source_name,
                        "url": item.get('url', f"{base_url}/events/"),
                        "latitude": 35.9942,
                        "longitude": -78.9030,
                    })
            except Exception:
                continue
        if not events:
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/",
                                         "DPAC, 123 Vivian St, Durham, NC", 35.9942, -78.9030, base_url)
        if not events:
            # DPAC renders plain <a href="/events/detail/..."> links with sibling <p> date text
            short_full_re = re.compile(
                r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+'
                r'(January|February|March|April|May|June|July|August|September|October|November|December)'
                r'\s+(\d{1,2}),?\s+(\d{4})',
                re.IGNORECASE
            )
            for a_tag in soup.find_all('a', href=re.compile(r'/events/detail/')):
                title_text = clean_text(a_tag.get_text())
                # fall back to img alt "More Info for <Title>"
                if not title_text or len(title_text) < 4:
                    img = a_tag.find('img')
                    if img:
                        alt = img.get('alt', '')
                        title_text = clean_text(re.sub(r'^More Info for\s+', '', alt, flags=re.IGNORECASE))
                if not title_text or len(title_text) < 4:
                    continue
                event_url = a_tag.get('href', f"{base_url}/events/")
                if event_url.startswith('/'):
                    event_url = base_url + event_url
                # Collect text from surrounding context: parent + siblings
                parent = a_tag.parent or a_tag
                context_text = parent.get_text()
                dm = short_full_re.search(context_text) or date_re.search(context_text)
                if not dm:
                    # try grandparent
                    gp = parent.parent
                    if gp:
                        context_text = gp.get_text()
                        dm = short_full_re.search(context_text) or date_re.search(context_text)
                if not dm:
                    continue
                month = dm.group(1).title()
                day = dm.group(2)
                year = int(dm.group(3)) if dm.lastindex >= 3 and dm.group(3) else datetime.now().year
                tm = time_re.search(context_text)
                event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:30 pm")
                if not event_date or event_date < cutoff:
                    continue
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": "DPAC, 123 Vivian St, Durham, NC",
                    "description": "",
                    "source": source_name,
                    "url": event_url,
                    "latitude": 35.9942,
                    "longitude": -78.9030,
                })
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ DPAC error: {e}")
    return events


async def scrape_nc_museum_of_art(session: aiohttp.ClientSession) -> List[Dict]:
    """ncartmuseum.org — North Carolina Museum of Art, Raleigh."""
    log_info("Scraping NC Museum of Art...")
    events = []
    source_name = "NC Museum of Art"
    base_url = "https://ncartmuseum.org"
    try:
        for path in ["/events/", "/programs-events/", "/calendar/"]:
            ical_raw = await fetch_url(f"{base_url}{path}?ical=1", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(
                    ical_raw, source_name,
                    "NC Museum of Art, 2110 Blue Ridge Rd, Raleigh, NC",
                    35.8027, -78.6893, f"{base_url}{path}"
                )
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
        html = await fetch_url(f"{base_url}/events/", session) or await fetch_with_geekflare(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(
            soup, source_name, f"{base_url}/events/",
            "NC Museum of Art, 2110 Blue Ridge Rd, Raleigh, NC",
            35.8027, -78.6893, base_url
        )
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ NC Museum of Art error: {e}")
    return events


async def scrape_cats_cradle(session: aiohttp.ClientSession) -> List[Dict]:
    """catscradle.com — Cat's Cradle live music venue in Carrboro."""
    log_info("Scraping Cat's Cradle (Carrboro)...")
    events = []
    source_name = "Cat's Cradle"
    base_url = "https://catscradle.com"
    location = "Cat's Cradle, 300 E Main St, Carrboro, NC"
    try:
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        events = _parse_seated_events(html, source_name, location, 35.9094, -79.0758, base_url)
        if not events:
            soup = BeautifulSoup(html, 'html.parser')
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", location, 35.9094, -79.0758, base_url)
        log_info(f"  ✓ Found {len(events)} Cat's Cradle events")
    except Exception as e:
        log_error(f"  ✗ Cat's Cradle error: {e}")
    return events


async def scrape_lincoln_theatre(session: aiohttp.ClientSession) -> List[Dict]:
    """lincolntheatre.com — Lincoln Theatre live music, Raleigh."""
    log_info("Scraping Lincoln Theatre (Raleigh)...")
    events = []
    source_name = "Lincoln Theatre"
    base_url = "https://www.lincolntheatre.com"
    location = "Lincoln Theatre, 126 E Cabarrus St, Raleigh, NC"
    try:
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        events = _parse_seated_events(html, source_name, location, 35.7740, -78.6376, base_url)
        if not events:
            soup = BeautifulSoup(html, 'html.parser')
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", location, 35.7740, -78.6376, base_url)
        log_info(f"  ✓ Found {len(events)} Lincoln Theatre events")
    except Exception as e:
        log_error(f"  ✗ Lincoln Theatre error: {e}")
    return events


async def scrape_raleigh_arts(session: aiohttp.ClientSession) -> List[Dict]:
    """raleighnc.gov/arts — City of Raleigh Arts Commission events."""
    log_info("Scraping Raleigh Arts Commission...")
    events = []
    source_name = "Raleigh Arts"
    try:
        url = "https://raleighnc.gov/arts/events"
        html = await fetch_url(url, session) or await fetch_with_geekflare(url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(soup, source_name, url, "Raleigh, NC", 35.7796, -78.6382, "https://raleighnc.gov")
        # JSON-LD fallback
        if not events:
            cutoff = datetime.now() - timedelta(hours=2)
            seen = set()
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        start_date = item.get('startDate', '')
                        if not title_text or not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < cutoff:
                            continue
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": "Raleigh, NC",
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', url),
                            "latitude": 35.7796,
                            "longitude": -78.6382,
                        })
                except Exception:
                    continue
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Raleigh Arts error: {e}")
    return events


async def scrape_duke_events(session: aiohttp.ClientSession) -> List[Dict]:
    """calendar.duke.edu — Duke University public events calendar."""
    log_info("Scraping Duke University events...")
    events = []
    source_name = "Duke Events"
    base_url = "https://calendar.duke.edu"
    try:
        # Duke calendar exposes iCal
        ical_url = f"{base_url}/events.ics?future=1"
        raw = await fetch_url(ical_url, session)
        if raw and 'BEGIN:VCALENDAR' in raw:
            events = parse_ical_feed(raw, source_name, "Duke University, Durham, NC", 35.9940, -78.8986, base_url)
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/events", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(soup, source_name, f"{base_url}/events", "Duke University, Durham, NC", 35.9940, -78.8986, base_url)
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Duke Events error: {e}")
    return events


async def scrape_unc_events(session: aiohttp.ClientSession) -> List[Dict]:
    """calendar.unc.edu — UNC Chapel Hill public events."""
    log_info("Scraping UNC Chapel Hill events...")
    events = []
    source_name = "UNC Events"
    base_url = "https://calendar.unc.edu"
    try:
        ical_url = f"{base_url}/feed/ical/"
        raw = await fetch_url(ical_url, session)
        if raw and 'BEGIN:VCALENDAR' in raw:
            events = parse_ical_feed(raw, source_name, "UNC Chapel Hill, Chapel Hill, NC", 35.9132, -79.0558, base_url)
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(soup, source_name, f"{base_url}/", "UNC Chapel Hill, Chapel Hill, NC", 35.9132, -79.0558, base_url)
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ UNC Events error: {e}")
    return events


async def scrape_marbles_kids_museum(session: aiohttp.ClientSession) -> List[Dict]:
    """marbleskidsmuseum.org — Marbles Kids Museum, Raleigh."""
    log_info("Scraping Marbles Kids Museum...")
    events = []
    source_name = "Marbles Kids Museum"
    base_url = "https://www.marbleskidsmuseum.org"
    try:
        for path in ["/events/", "/calendar/", "/"]:
            ical_raw = await fetch_url(f"{base_url}{path}?ical=1", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(
                    ical_raw, source_name,
                    "Marbles Kids Museum, 201 E Hargett St, Raleigh, NC",
                    35.7760, -78.6350, f"{base_url}/events/"
                )
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(
            soup, source_name, f"{base_url}/events/",
            "Marbles Kids Museum, 201 E Hargett St, Raleigh, NC",
            35.7760, -78.6350, base_url
        )
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Marbles Kids Museum error: {e}")
    return events


async def scrape_morehead_planetarium(session: aiohttp.ClientSession) -> List[Dict]:
    """moreheadplanetarium.org — Morehead Planetarium & Science Center, Chapel Hill."""
    log_info("Scraping Morehead Planetarium...")
    events = []
    source_name = "Morehead Planetarium"
    base_url = "https://moreheadplanetarium.org"
    try:
        for path in ["/events/", "/shows/", "/calendar/"]:
            ical_raw = await fetch_url(f"{base_url}{path}?ical=1", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(
                    ical_raw, source_name,
                    "Morehead Planetarium, 250 E Franklin St, Chapel Hill, NC",
                    35.9133, -79.0521, f"{base_url}{path}"
                )
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(
            soup, source_name, f"{base_url}/events/",
            "Morehead Planetarium, 250 E Franklin St, Chapel Hill, NC",
            35.9133, -79.0521, base_url
        )
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Morehead Planetarium error: {e}")
    return events


async def scrape_nc_state_events(session: aiohttp.ClientSession) -> List[Dict]:
    """calendar.ncsu.edu — NC State University public events."""
    log_info("Scraping NC State events...")
    events = []
    source_name = "NC State Events"
    base_url = "https://calendar.ncsu.edu"
    try:
        ical_url = f"{base_url}/feed/ical/"
        raw = await fetch_url(ical_url, session)
        if raw and 'BEGIN:VCALENDAR' in raw:
            events = parse_ical_feed(raw, source_name, "NC State University, Raleigh, NC", 35.7847, -78.6821, base_url)
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        events = _parse_tribe_events(soup, source_name, f"{base_url}/", "NC State University, Raleigh, NC", 35.7847, -78.6821, base_url)
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ NC State Events error: {e}")
    return events


async def scrape_eventbrite_triangle(session: aiohttp.ClientSession) -> List[Dict]:
    """Eventbrite listings for Raleigh, Durham, and Chapel Hill."""
    log_info("Scraping Eventbrite Triangle area...")
    events = []
    source_name = "Eventbrite"
    urls = [
        "https://www.eventbrite.com/d/nc--raleigh/events/",
        "https://www.eventbrite.com/d/nc--durham/events/",
        "https://www.eventbrite.com/d/nc--chapel-hill/events/",
    ]
    # Coordinates indexed by URL
    coords = {
        "nc--raleigh":     (35.7796, -78.6382, "Raleigh, NC"),
        "nc--durham":      (35.9940, -78.8986, "Durham, NC"),
        "nc--chapel-hill": (35.9132, -79.0558, "Chapel Hill, NC"),
    }
    try:
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        for url in urls:
            city_key = next((k for k in coords if k in url), None)
            lat, lon, default_loc = coords.get(city_key, (35.7796, -78.6382, "Triangle, NC"))
            html = await fetch_url(url, session)
            if not html or len(html) < 1000 or any(x in html for x in ['Verify you are a human', 'cf-challenge', 'Just a moment']):
                html = await fetch_with_geekflare(url, session)
            if not html or len(html) < 1000:
                continue
            for jsonld_str in re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            ):
                try:
                    data = json.loads(jsonld_str.strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict) or 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        if not title_text or len(title_text) < 4:
                            continue
                        start_date = item.get('startDate', '')
                        if not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < cutoff:
                            continue
                        location = default_loc
                        loc_obj = item.get('location', {})
                        if isinstance(loc_obj, dict):
                            venue_name = clean_text(loc_obj.get('name', ''))
                            addr_obj = loc_obj.get('address', {})
                            if isinstance(addr_obj, dict):
                                parts = [p for p in [venue_name, addr_obj.get('streetAddress', ''), addr_obj.get('addressLocality', ''), addr_obj.get('addressRegion', '')] if p]
                                if parts:
                                    location = ', '.join(parts)[:100]
                            elif venue_name:
                                location = venue_name
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', url),
                            "latitude": lat,
                            "longitude": lon,
                        })
                except Exception:
                    continue
        if events:
            log_info(f"  ✓ Found {len(events)} Eventbrite events")
        else:
            log_warn("  ⚠ Eventbrite: no events found (bot protection likely active)")
    except Exception as e:
        log_error(f"  ✗ Eventbrite error: {e}")
    return events


async def scrape_americantowns_triangle(session: aiohttp.ClientSession) -> List[Dict]:
    """AmericanTowns event listings for Wake, Durham, and Orange counties."""
    log_info("Scraping AmericanTowns Triangle counties...")
    events = []
    source_name = "AmericanTowns"
    counties = [
        ("https://www.americantowns.com/wake-county-nc/events/",   "AmericanTowns Wake",   35.7796, -78.6382, "Wake County, NC"),
        ("https://www.americantowns.com/durham-county-nc/events/", "AmericanTowns Durham", 35.9940, -78.8986, "Durham County, NC"),
        ("https://www.americantowns.com/orange-county-nc/events/", "AmericanTowns Orange", 35.9132, -79.0558, "Orange County, NC"),
    ]
    cutoff = datetime.now() - timedelta(hours=2)
    date_re = re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2})(?:,?\s+(\d{4}))?', re.IGNORECASE
    )
    for url, label, lat, lon, default_loc in counties:
        html = await fetch_url(url, session)
        if not html:
            html = await fetch_with_geekflare(url, session)
        if not html:
            continue
        try:
            soup = BeautifulSoup(html, 'html.parser')
            seen = set()
            event_items = (
                soup.find_all("div", class_=lambda c: c and "event" in c.lower())
                or soup.find_all("li", class_=lambda c: c and "event" in c.lower())
                or soup.find_all("article", class_=lambda c: c and "event" in c.lower())
            )
            if event_items:
                for item in event_items:
                    title_el = item.find("h2") or item.find("h3") or item.find("h4") or item.find(class_=lambda c: c and "title" in c.lower())
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not title:
                        continue
                    date_el = item.find("time") or item.find(class_=lambda c: c and "date" in c.lower())
                    date_str = (date_el.get("datetime", "") or date_el.get_text(strip=True)) if date_el else ""
                    event_date = None
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"]:
                        try:
                            event_date = datetime.strptime(date_str.strip(), fmt)
                            break
                        except ValueError:
                            continue
                    if not event_date:
                        dm = date_re.search(item.get_text())
                        if dm:
                            event_date = parse_date_time(dm.group(1), dm.group(2), int(dm.group(3)) if dm.group(3) else None)
                    if not event_date or event_date < cutoff:
                        continue
                    key = f"{title.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    link_el = item.find("a", href=True)
                    link = link_el["href"] if link_el else url
                    if link and link.startswith("/"):
                        link = f"https://www.americantowns.com{link}"
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), label),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": default_loc,
                        "description": "",
                        "source": label,
                        "url": link,
                        "latitude": lat,
                        "longitude": lon,
                    })
            if not event_items or not events:
                # Plain date regex fallback
                for month, day, year_str in re.findall(
                    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,?\s+(\d{4}))?',
                    html, re.IGNORECASE
                )[:20]:
                    year = int(year_str) if year_str else None
                    event_date = parse_date_time(month, day, year)
                    if event_date and event_date >= cutoff:
                        events.append({
                            "id": create_event_id(f"Event {month} {day}", event_date.isoformat(), label),
                            "title": f"Local Event — {default_loc}",
                            "date": event_date.isoformat(),
                            "location": default_loc,
                            "description": "",
                            "source": label,
                            "url": url,
                            "latitude": lat,
                            "longitude": lon,
                        })
            log_info(f"  ✓ {label}: {len(events)} events so far")
        except Exception as e:
            log_error(f"  ✗ {label} parse error: {e}")
    return events


async def scrape_raleigh_little_theatre(session: aiohttp.ClientSession) -> List[Dict]:
    """raleighlittletheatre.org — oldest community theatre in NC."""
    log_info("Scraping Raleigh Little Theatre...")
    events = []
    source_name = "Raleigh Little Theatre"
    base_url = "https://raleighlittletheatre.org"
    location = "Raleigh Little Theatre, 301 Pogue St, Raleigh, NC"
    try:
        # RLT uses /shows/ for their season listing (not /events/?ical=1)
        for path in ["/shows/", "/events/"]:
            html = await fetch_url(f"{base_url}{path}", session)
            if not html:
                continue
            # Try iCal param first
            ical_raw = await fetch_url(f"{base_url}{path}?ical=1", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(ical_raw, source_name, location, 35.7860, -78.6580, f"{base_url}{path}")
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
            parsed = _parse_show_run_events(html, source_name, location, 35.7860, -78.6580, base_url, r'/shows?/')
            if parsed:
                events.extend(parsed)
                break
            soup = BeautifulSoup(html, 'html.parser')
            tribe = _parse_tribe_events(soup, source_name, f"{base_url}{path}", location, 35.7860, -78.6580, base_url)
            if tribe:
                events.extend(tribe)
                break
        log_info(f"  ✓ Found {len(events)} Raleigh Little Theatre events")
    except Exception as e:
        log_error(f"  ✗ Raleigh Little Theatre error: {e}")
    return events


async def scrape_playmakers_theatre(session: aiohttp.ClientSession) -> List[Dict]:
    """playmakersrep.org — PlayMakers Repertory Company, Chapel Hill."""
    log_info("Scraping PlayMakers Repertory Company...")
    events = []
    source_name = "PlayMakers Rep"
    base_url = "https://playmakersrep.org"
    location = "PlayMakers Repertory, Paul Green Theatre, Chapel Hill, NC"
    try:
        now = datetime.now()
        # Try current and upcoming seasons
        seasons = [
            f"{now.year}-{now.year + 1}",
            f"{now.year - 1}-{now.year}",
        ]
        for season in seasons:
            html = await fetch_url(f"{base_url}/season/{season}/", session)
            if html and len(html) > 500:
                parsed = _parse_show_run_events(html, source_name, location, 35.9052, -79.0505, base_url, r'/show/')
                if parsed:
                    events.extend(parsed)
                    break
        if not events:
            # Fallback to generic events page
            html = await fetch_url(f"{base_url}/events/", session)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", location, 35.9052, -79.0505, base_url)
        log_info(f"  ✓ Found {len(events)} PlayMakers events")
    except Exception as e:
        log_error(f"  ✗ PlayMakers Rep error: {e}")
    return events


async def scrape_artspace_raleigh(session: aiohttp.ClientSession) -> List[Dict]:
    """artspacenc.org — Artspace visual arts center, downtown Raleigh."""
    log_info("Scraping Artspace Raleigh...")
    events = []
    source_name = "Artspace Raleigh"
    base_url = "https://artspacenc.org"
    location = "Artspace, 201 E Davie St, Raleigh, NC"
    try:
        html = await fetch_url(f"{base_url}/events/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        # Artspace format: <h4><a href="...">Title</a></h4>  followed by "17 May | 11:00 am"
        day_month_re = re.compile(
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'(?:\s+(\d{4}))?\s*\|?\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))?',
            re.IGNORECASE
        )
        for h4 in soup.find_all('h4'):
            link = h4.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 4:
                continue
            event_url = link.get('href', f"{base_url}/events/")
            if event_url.startswith('/'):
                event_url = base_url + event_url
            # Search parent/grandparent text for date (date appears above h4 as preceding sibling)
            block = h4.parent or h4
            block_text = block.get_text()
            dm = day_month_re.search(block_text)
            if not dm and block.parent:
                block_text = block.parent.get_text()
                dm = day_month_re.search(block_text)
            if not dm:
                continue
            day = dm.group(1)
            month = dm.group(2).title()
            year = int(dm.group(3)) if dm.group(3) else datetime.now().year
            time_str = dm.group(4) if dm.group(4) else "11:00 am"
            event_date = parse_date_time(month, day, year=year, time_str=time_str)
            if not event_date or event_date < cutoff:
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": location,
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 35.7762,
                "longitude": -78.6357,
            })
        if not events:
            events = _parse_tribe_events(soup, source_name, f"{base_url}/events/", location, 35.7762, -78.6357, base_url)
        log_info(f"  ✓ Found {len(events)} Artspace events")
    except Exception as e:
        log_error(f"  ✗ Artspace Raleigh error: {e}")
    return events


async def scrape_carolina_hurricanes(session: aiohttp.ClientSession) -> List[Dict]:
    """Carolina Hurricanes home game schedule via NHL API (no key required)."""
    log_info("Scraping Carolina Hurricanes schedule...")
    events = []
    source_name = "Carolina Hurricanes"
    try:
        now = datetime.now()
        cutoff = now - timedelta(hours=2)
        seen = set()
        # Fetch current month + next 8 months to cover full season
        for offset in range(9):
            month_dt = now.replace(day=1) + timedelta(days=offset * 31)
            ym = month_dt.strftime('%Y-%m')
            url = f"https://api-web.nhle.com/v1/club-schedule/CAR/month/{ym}"
            raw = await fetch_url(url, session, extra_headers={'Accept': 'application/json'})
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            for game in data.get('games', []):
                # Only home games
                if game.get('homeTeam', {}).get('abbrev', '') != 'CAR':
                    continue
                start_utc = game.get('startTimeUTC', '')
                game_date_str = game.get('gameDate', '')
                if not start_utc and not game_date_str:
                    continue
                try:
                    if start_utc:
                        # Convert UTC to Eastern (UTC-4 summer, UTC-5 winter)
                        game_dt = datetime.strptime(start_utc[:16], '%Y-%m-%dT%H:%M')
                        eastern_offset = -4 if 3 <= game_dt.month <= 10 else -5
                        game_dt = game_dt + timedelta(hours=eastern_offset)
                    else:
                        game_dt = datetime.strptime(game_date_str, '%Y-%m-%d')
                        game_dt = game_dt.replace(hour=19, minute=0)
                except Exception:
                    continue
                if game_dt < cutoff:
                    continue
                away_place = game.get('awayTeam', {}).get('placeName', {}).get('default', '')
                away_name = game.get('awayTeam', {}).get('commonName', {}).get('default', '')
                opponent = away_place or away_name or 'Opponent'
                game_type = game.get('gameType', 2)
                type_label = "Playoff Game" if game_type == 3 else "vs."
                title = f"Carolina Hurricanes {type_label} {opponent}"
                key = f"{title.lower()}_{game_dt.date()}"
                if key in seen:
                    continue
                seen.add(key)
                venue = game.get('venue', {}).get('default', 'Lenovo Center')
                events.append({
                    "id": create_event_id(title, game_dt.isoformat(), source_name),
                    "title": title,
                    "date": game_dt.isoformat(),
                    "location": f"{venue}, Raleigh, NC",
                    "description": "Carolina Hurricanes NHL hockey game.",
                    "source": source_name,
                    "url": "https://www.nhl.com/hurricanes/schedule",
                    "latitude": 35.8033,
                    "longitude": -78.7228,
                })
        log_info(f"  ✓ Found {len(events)} Hurricanes home games")
    except Exception as e:
        log_error(f"  ✗ Carolina Hurricanes error: {e}")
    return events


async def scrape_booth_amphitheatre(session: aiohttp.ClientSession) -> List[Dict]:
    """boothamphitheatre.com — Booth Amphitheatre, Cary (outdoor summer venue)."""
    log_info("Scraping Booth Amphitheatre (Cary)...")
    events = []
    source_name = "Booth Amphitheatre"
    base_url = "https://www.boothamphitheatre.com"
    location = "Booth Amphitheatre, 8003 Regency Pkwy, Cary, NC"
    try:
        html = await fetch_url(f"{base_url}/events", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        # Date format: "May 22, 2026"
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\s+(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        skip = {'events', 'more info', 'buy tickets', 'all events'}
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            a = heading.find('a')
            title_text = clean_text(heading.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip:
                continue
            event_url = (a.get('href', '') if a else '') or f"{base_url}/events"
            if event_url.startswith('/'):
                event_url = base_url + event_url
            block = heading.parent or heading
            block_text = block.get_text()
            dm = date_re.search(block_text)
            if not dm and block.parent:
                block_text = block.parent.get_text()
                dm = date_re.search(block_text)
            if not dm:
                continue
            month, day, year = dm.group(1).title(), dm.group(2), int(dm.group(3))
            tm = time_re.search(block_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 pm")
            if not event_date or event_date < cutoff or event_date.year != year:
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": location,
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 35.7916,
                "longitude": -78.8152,
            })
        log_info(f"  ✓ Found {len(events)} Booth Amphitheatre events")
    except Exception as e:
        log_error(f"  ✗ Booth Amphitheatre error: {e}")
    return events


async def scrape_red_hat_amphitheater(session: aiohttp.ClientSession) -> List[Dict]:
    """redhatamphitheater.com — Red Hat Amphitheater, downtown Raleigh."""
    log_info("Scraping Red Hat Amphitheater (Raleigh)...")
    events = []
    source_name = "Red Hat Amphitheater"
    base_url = "https://www.redhatamphitheater.com"
    location = "Red Hat Amphitheater, 500 S McDowell St, Raleigh, NC"
    try:
        html = await fetch_url(f"{base_url}/events", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        # Date format: "Sat. May 16, 2026 7:30 PM"
        date_re = re.compile(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December|'
            r'Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
            r'\s+(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        skip = {'events', 'more info', 'buy tickets', 'all events', 'tickets'}
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            a = heading.find('a')
            title_text = clean_text(heading.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip:
                continue
            event_url = (a.get('href', '') if a else '') or f"{base_url}/events"
            if event_url.startswith('/'):
                event_url = base_url + event_url
            # Date is often in a sibling element — try parent then grandparent
            block = heading.parent or heading
            block_text = block.get_text()
            dm = date_re.search(block_text)
            if not dm and block.parent:
                block_text = block.parent.get_text()
                dm = date_re.search(block_text)
            if not dm:
                continue
            month = _MONTH_ABBR.get(dm.group(1)[:3].lower(), dm.group(1).title())
            day, year = dm.group(2), int(dm.group(3))
            tm = time_re.search(block_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 pm")
            if not event_date or event_date < cutoff or event_date.year != year:
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": location,
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 35.7737,
                "longitude": -78.6383,
            })
        log_info(f"  ✓ Found {len(events)} Red Hat Amphitheater events")
    except Exception as e:
        log_error(f"  ✗ Red Hat Amphitheater error: {e}")
    return events


async def scrape_the_ritz(session: aiohttp.ClientSession) -> List[Dict]:
    """ritzraleigh.com — The Ritz, Raleigh music venue."""
    log_info("Scraping The Ritz (Raleigh)...")
    events = []
    source_name = "The Ritz"
    base_url = "https://www.ritzraleigh.com"
    location = "The Ritz, 2820 Industrial Dr, Raleigh, NC"
    try:
        html = await fetch_url(base_url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        # Try JSON-LD first
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if 'Event' not in item.get('@type', ''):
                        continue
                    title_text = clean_text(item.get('name', ''))
                    start_date = item.get('startDate', '')
                    if not title_text or not start_date:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                        event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": location,
                        "description": clean_text(item.get('description', ''))[:200],
                        "source": source_name,
                        "url": item.get('url', base_url),
                        "latitude": 35.8468,
                        "longitude": -78.6379,
                    })
            except Exception:
                continue
        if not events:
            # HTML fallback: date split as "Fri\n24\nJul" across sibling divs above the title link
            date_re = re.compile(
                r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[\s\n]+(\d{1,2})[\s\n]+'
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:[\s\n]+(\d{4}))?',
                re.IGNORECASE
            )
            time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
            for a_tag in soup.find_all('a', href=re.compile(r'ritzraleigh\.com|/event')):
                title_text = clean_text(a_tag.get_text())
                if not title_text or len(title_text) < 4:
                    continue
                event_url = a_tag.get('href', base_url)
                if event_url.startswith('/'):
                    event_url = base_url + event_url
                # Walk up to find the event card container that includes the date
                block = a_tag.parent
                block_text = block.get_text() if block else ''
                dm = date_re.search(block_text)
                if not dm and block and block.parent:
                    block_text = block.parent.get_text()
                    dm = date_re.search(block_text)
                if not dm:
                    continue
                day = dm.group(1)
                month = _MONTH_ABBR.get(dm.group(2)[:3].lower(), dm.group(2).title())
                year = int(dm.group(3)) if dm.group(3) else datetime.now().year
                tm = time_re.search(block_text)
                event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "8:00 pm")
                if not event_date or event_date < cutoff:
                    continue
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": location,
                    "description": "",
                    "source": source_name,
                    "url": event_url,
                    "latitude": 35.8468,
                    "longitude": -78.6379,
                })
        log_info(f"  ✓ Found {len(events)} The Ritz events")
    except Exception as e:
        log_error(f"  ✗ The Ritz error: {e}")
    return events


async def scrape_motorco(session: aiohttp.ClientSession) -> List[Dict]:
    """motorcomusic.com — Motorco Music Hall, Durham."""
    log_info("Scraping Motorco Music Hall (Durham)...")
    events = []
    source_name = "Motorco Music Hall"
    base_url = "https://motorcomusic.com"
    location = "Motorco Music Hall, 723 Rigsbee Ave, Durham, NC"
    try:
        for path in ["/calendar/", "/events/"]:
            ical_raw = await fetch_url(f"{base_url}{path}?ical=1", session)
            if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
                events = parse_ical_feed(ical_raw, source_name, location, 35.9977, -78.9047, f"{base_url}{path}")
                if events:
                    log_info(f"  ✓ Found {len(events)} events via iCal")
                    return events
            html = await fetch_url(f"{base_url}{path}", session)
            if not html:
                continue
            events = _parse_seated_events(html, source_name, location, 35.9977, -78.9047, base_url)
            if not events:
                soup = BeautifulSoup(html, 'html.parser')
                events = _parse_tribe_events(soup, source_name, f"{base_url}{path}", location, 35.9977, -78.9047, base_url)
            if events:
                break
        log_info(f"  ✓ Found {len(events)} Motorco events")
    except Exception as e:
        log_error(f"  ✗ Motorco error: {e}")
    return events


async def scrape_durham_bulls(session: aiohttp.ClientSession) -> List[Dict]:
    """Durham Bulls home game schedule via MLB stats API (no key required)."""
    log_info("Scraping Durham Bulls home games...")
    events = []
    source_name = "Durham Bulls"
    try:
        now = datetime.now()
        start_date = now.strftime('%Y-%m-%d')
        end_date = (now + timedelta(days=120)).strftime('%Y-%m-%d')
        # Durham Bulls team ID = 234 in the MLB stats system (Triple-A affiliate)
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?teamId=234&sportId=11&startDate={start_date}&endDate={end_date}"
            f"&hydrate=team,venue&gameTypes=R,F,D,L,W"
        )
        raw = await fetch_url(url, session, extra_headers={'Accept': 'application/json'})
        if not raw:
            return events
        data = json.loads(raw)
        cutoff = now - timedelta(hours=2)
        seen = set()
        for date_entry in data.get('dates', []):
            for game in date_entry.get('games', []):
                home = game.get('teams', {}).get('home', {}).get('team', {})
                if 'Durham' not in home.get('name', ''):
                    continue
                game_date_str = game.get('gameDate', '')
                if not game_date_str:
                    continue
                try:
                    # gameDate is UTC ISO format, display as-is (local time TBD)
                    game_date = datetime.strptime(game_date_str[:16], '%Y-%m-%dT%H:%M')
                except Exception:
                    continue
                if game_date < cutoff:
                    continue
                away = game.get('teams', {}).get('away', {}).get('team', {}).get('name', 'Opponent')
                title = f"Durham Bulls vs. {away}"
                key = f"{title.lower()}_{game_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                venue_name = game.get('venue', {}).get('name', 'Durham Bulls Athletic Park')
                events.append({
                    "id": create_event_id(title, game_date.isoformat(), source_name),
                    "title": title,
                    "date": game_date.isoformat(),
                    "location": f"{venue_name}, Durham, NC",
                    "description": "Durham Bulls minor league baseball — Triple-A affiliate of the Tampa Bay Rays.",
                    "source": source_name,
                    "url": "https://www.milb.com/durham",
                    "latitude": 35.9883,
                    "longitude": -78.8986,
                })
        log_info(f"  ✓ Found {len(events)} Durham Bulls home games")
    except Exception as e:
        log_error(f"  ✗ Durham Bulls error: {e}")
    return events


# ─────────────────────────────────────────────
# Manual Events
# ─────────────────────────────────────────────
def load_manual_events() -> List[Dict]:
    try:
        with open(MANUAL_EVENTS_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else data.get("events", [])
    except FileNotFoundError:
        return []
    except Exception as e:
        log_error(f"  ✗ Could not load manual_events.json: {e}")
        return []


async def scrape_manual_events(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Loading manual events...")
    events = []
    try:
        raw_events = load_manual_events()
        cutoff = datetime.now() - timedelta(hours=2)
        date_formats = [
            "%Y-%m-%dT%H:%M:%S", "%B %d, %Y at %I:%M %p",
            "%B %d, %Y at %I %p", "%B %d, %Y", "%Y-%m-%d",
        ]
        for event in raw_events:
            if not event.get("title") or not event.get("date"):
                continue
            event_date = None
            for fmt in date_formats:
                try:
                    event_date = datetime.strptime(event["date"].strip(), fmt)
                    break
                except ValueError:
                    continue
            if not event_date or event_date < cutoff:
                continue
            event["date"] = event_date.isoformat()
            event.setdefault("id", create_event_id(event["title"], event["date"], "Manual"))
            event.setdefault("source", "Manual")
            event.setdefault("url", "")
            event.setdefault("description", "")
            event.setdefault("location", "Triangle, NC")
            event.setdefault("latitude", 35.7796)
            event.setdefault("longitude", -78.6382)
            events.append(event)
        log_info(f"  ✓ Loaded {len(events)} manual events")
    except Exception as e:
        log_error(f"  ✗ Error loading manual events: {e}")
    return events


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    print("=" * 60)
    print("Triangle Happenings — Event Scraper Starting")
    print("=" * 60)

    all_events: List[Dict] = []

    scrapers = [
        ("Manual Events",           scrape_manual_events),
        ("Visit Raleigh",           scrape_visit_raleigh),
        ("Indy Week",               scrape_indy_week),
        ("Visit Durham",            scrape_visit_durham),
        ("Chapelboro",              scrape_chapelboro),
        ("DPAC",                    scrape_dpac),
        ("NC Museum of Art",        scrape_nc_museum_of_art),
        ("Cat's Cradle",            scrape_cats_cradle),
        ("Lincoln Theatre",         scrape_lincoln_theatre),
        ("Raleigh Arts",            scrape_raleigh_arts),
        ("Duke Events",             scrape_duke_events),
        ("UNC Events",              scrape_unc_events),
        ("NC State Events",         scrape_nc_state_events),
        ("Marbles Kids Museum",     scrape_marbles_kids_museum),
        ("Morehead Planetarium",    scrape_morehead_planetarium),
        ("Raleigh Little Theatre",  scrape_raleigh_little_theatre),
        ("PlayMakers Rep",          scrape_playmakers_theatre),
        ("Artspace Raleigh",        scrape_artspace_raleigh),
        ("Durham Bulls",            scrape_durham_bulls),
        ("Carolina Hurricanes",     scrape_carolina_hurricanes),
        ("Booth Amphitheatre",      scrape_booth_amphitheatre),
        ("Red Hat Amphitheater",    scrape_red_hat_amphitheater),
        ("The Ritz",                scrape_the_ritz),
    ]

    # Load previous source counts and zero-strike counts for regression detection
    prev_counts: Dict[str, int] = {}
    zero_strikes: Dict[str, int] = {}   # how many consecutive runs a source has returned 0
    try:
        with open(OUTPUT_FILE) as f:
            prev_data = json.load(f)
        prev_counts = prev_data.get("source_counts", {})
        zero_strikes = prev_data.get("zero_strikes", {})
    except Exception:
        pass

    source_counts: Dict[str, int] = {}
    async with aiohttp.ClientSession() as session:
        for source_name, scraper_func in scrapers:
            try:
                events = await scraper_func(session)
                all_events.extend(events)
                source_counts[source_name] = len(events)
                print(f"  → {source_name}: {len(events)} events")
            except Exception as e:
                log_error(f"  ✗ {source_name} failed: {e}")
                source_counts[source_name] = 0

    print("\nDeduplication...")
    original_count = len(all_events)
    all_events = deduplicate_events(all_events)
    removed = original_count - len(all_events)
    print(f"  ✓ Removed {removed} duplicates" if removed else "  ✓ No duplicates found")

    all_events.sort(key=lambda x: x["date"])
    now = datetime.now()
    cutoff = now + timedelta(days=365)
    all_events = [
        e for e in all_events
        if datetime.fromisoformat(e["date"]) >= now - timedelta(hours=2)
        and datetime.fromisoformat(e["date"]) <= cutoff
    ]
    print(f"  • Filtered to {len(all_events)} future events (next 365 days)")

    # Update consecutive-zero-strike counters
    current_strikes: Dict[str, int] = {}
    for name, _ in scrapers:
        count = source_counts.get(name, 0)
        if count == 0 and prev_counts.get(name, 0) >= 3:
            current_strikes[name] = zero_strikes.get(name, 0) + 1
        else:
            current_strikes[name] = 0

    output = {
        "events": all_events,
        "last_updated": now.isoformat(),
        "total_events": len(all_events),
        "sources": [name for name, _ in scrapers],
        "source_counts": source_counts,
        "zero_strikes": current_strikes,
    }

    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_FILE)), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(all_events)} events → triangle_happenings.json")

    # ── Health report ─────────────────────────────────────────────
    # Sources that are exempt from regression alerts (expected to be 0 sometimes)
    exempt = {"Manual Events", "Eventbrite Triangle"}

    regressions = []
    print("\n" + "=" * 60)
    print("SCRAPER HEALTH REPORT")
    print("=" * 60)
    print(f"{'Source':<30} {'Now':>6} {'Prev':>6} {'Strike':>6}  Status")
    print("-" * 65)
    for name, _ in scrapers:
        count  = source_counts.get(name, 0)
        prev   = prev_counts.get(name, None)
        strike = current_strikes.get(name, 0)
        if prev is None:
            status = "NEW"
        elif count == 0 and strike >= 2 and name not in exempt:
            # Only alert after 2 consecutive zero runs (avoids single-run network blips)
            status = "🔴 BROKEN"
            regressions.append((name, prev))
        elif count == 0 and strike == 1 and name not in exempt:
            status = "⚠ WARN (1 zero run)"
        elif count < prev * 0.25 and prev >= 10 and name not in exempt:
            status = f"⚠ LOW ({count} vs {prev} last run)"
            regressions.append((name, prev))
        elif count == 0:
            status = "EMPTY"
        else:
            status = "✅ OK"
        prev_str = str(prev) if prev is not None else "—"
        print(f"  {name:<28} {count:>6} {prev_str:>6} {strike:>6}  {status}")

    # Write GitHub Actions step summary if running in CI
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as sf:
            sf.write("## Triangle Happenings Scraper Health\n\n")
            sf.write(f"**Total events:** {len(all_events)}  \n")
            sf.write(f"**Run at:** {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n")
            sf.write("| Source | Events | Prev | Strikes | Status |\n")
            sf.write("|--------|-------:|-----:|--------:|--------|\n")
            for name, _ in scrapers:
                count  = source_counts.get(name, 0)
                prev   = prev_counts.get(name, None)
                strike = current_strikes.get(name, 0)
                prev_str = str(prev) if prev is not None else "—"
                if name in [r[0] for r in regressions]:
                    status = "🔴 BROKEN"
                elif strike == 1:
                    status = "🟡 Warning"
                elif count == 0:
                    status = "⚪ Empty"
                else:
                    status = "🟢 OK"
                sf.write(f"| {name} | {count} | {prev_str} | {strike} | {status} |\n")
            if regressions:
                sf.write(f"\n### ⚠️ {len(regressions)} broken source(s)\n\n")
                for name, prev in regressions:
                    sf.write(f"- **{name}** had {prev} events last run, now 0\n")

    if regressions:
        print("\n" + "=" * 60)
        print(f"ERROR: {len(regressions)} scraper(s) stopped working:")
        for name, prev in regressions:
            print(f"  • {name} — had {prev} events, now 0")
        print("=" * 60)
        import sys
        sys.exit(1)

    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
