"""Portal Inmobiliario site service.

Orchestrates browser fetching + parsing for this specific portal.
Contains all site-specific knowledge not covered by generic parsers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from src.browser import BrowserManager
from src.config import Settings, get_settings, load_selectors
from src.logging_config import get_logger
from src.models import Listing, SearchCard
from src.parsers.detail_parser import parse_detail_page
from src.parsers.search_parser import parse_search_page, parse_total_results
from src.utils import save_raw_html, save_raw_json

logger = get_logger(__name__)

# Selector to wait for before declaring the search page "loaded"
_SEARCH_READY_SELECTOR = "li.ui-search-layout__item, ol.ui-search-layout, .ui-search-results"
_DETAIL_READY_SELECTOR = "h1.ui-pdp-title, .ui-pdp-price, .ui-pdp-header"


class PortalInmobiliarioService:
    """High-level operations against Portal Inmobiliario."""

    def __init__(
        self,
        browser: BrowserManager,
        settings: Optional[Settings] = None,
        run_id: str = "default",
    ) -> None:
        self.browser = browser
        self.settings = settings or get_settings()
        self.run_id = run_id

    async def fetch_search_page(
        self, url: str
    ) -> tuple[list[SearchCard], Optional[str], Optional[int]]:
        """Fetch and parse one search result page.

        Returns:
            (cards, next_page_url, total_count)
        """
        logger.info("fetch_search_page", url=url)
        try:
            html = await self.browser.fetch_page(
                url,
                wait_selector=_SEARCH_READY_SELECTOR,
                save_screenshot_on_error=True,
                screenshot_dir=self.settings.log_dir / "screenshots",
            )
        except Exception as exc:
            logger.error("search_page_fetch_failed", url=url, error=str(exc))
            return [], None, None

        # Save raw HTML for debugging
        if self.settings.save_raw:
            try:
                raw_path = save_raw_html(html, url, self.run_id, self.settings.raw_dir)
                logger.debug("raw_html_saved", path=str(raw_path))
            except Exception as exc:
                logger.warning("raw_html_save_failed", error=str(exc))

        cards, next_page_url = parse_search_page(html, search_url=url)
        total = parse_total_results(html)
        return cards, next_page_url, total

    async def fetch_all_search_pages(
        self,
        start_url: str,
        max_pages: Optional[int] = None,
        max_listings: Optional[int] = None,
    ) -> list[SearchCard]:
        """Paginate through all search result pages and collect SearchCards."""
        max_pages = max_pages or self.settings.max_pages
        max_listings = max_listings or self.settings.max_listings

        all_cards: list[SearchCard] = []
        url: Optional[str] = start_url
        page_num = 0

        while url and page_num < max_pages:
            page_num += 1
            logger.info("search_page", page=page_num, url=url)

            cards, next_url, total = await self.fetch_search_page(url)

            if not cards:
                logger.warning("search_page_empty", page=page_num, url=url)
                break

            all_cards.extend(cards)
            logger.info(
                "search_page_done",
                page=page_num,
                cards_this_page=len(cards),
                total_collected=len(all_cards),
                total_available=total,
            )

            if max_listings and len(all_cards) >= max_listings:
                logger.info("max_listings_reached", limit=max_listings)
                all_cards = all_cards[:max_listings]
                break

            url = next_url

        return all_cards

    async def fetch_detail_page(
        self,
        card: SearchCard,
    ) -> Optional[Listing]:
        """Fetch and parse one detail page, returning a full Listing."""
        url = card.listing_url
        logger.info("fetch_detail_page", listing_id=card.listing_id, url=url)

        raw_html_path: Optional[str] = None

        try:
            html = await self.browser.fetch_page(
                url,
                wait_selector=_DETAIL_READY_SELECTOR,
                save_screenshot_on_error=True,
                screenshot_dir=self.settings.log_dir / "screenshots",
            )
        except Exception as exc:
            logger.error("detail_page_fetch_failed", url=url, error=str(exc))
            return None

        # Save raw HTML
        if self.settings.save_raw:
            try:
                raw_path = save_raw_html(html, url, self.run_id, self.settings.raw_dir)
                raw_html_path = str(raw_path)
            except Exception as exc:
                logger.warning("raw_html_save_failed", error=str(exc))

        listing = parse_detail_page(
            html=html,
            listing_url=url,
            search_url=card.search_url,
            listing_position=card.listing_position,
            run_id=self.run_id,
            raw_html_path=raw_html_path,
        )

        if listing is None:
            logger.warning("detail_parse_returned_none", url=url)
            return None

        # Merge lightweight card data for fields that were extracted on the search page
        # but might be missing in the detail (e.g. listing_position)
        if listing.listing_position is None:
            listing.listing_position = card.listing_position
        if listing.title is None and card.title:
            listing.title = card.title

        logger.info(
            "detail_parsed",
            listing_id=listing.listing_id,
            price=listing.price,
            currency=listing.currency,
            bedrooms=listing.bedrooms,
        )
        return listing
