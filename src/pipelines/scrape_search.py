"""Pipeline: collect SearchCards from one or more search URLs."""
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.browser import browser_session
from src.config import Settings, get_settings
from src.logging_config import get_logger
from src.models import ScraperRun, SearchCard
from src.services.portal_inmobiliario import PortalInmobiliarioService
from src.utils import append_checkpoint, load_checkpoint

logger = get_logger(__name__)


async def run_search_pipeline(
    search_urls: list[str],
    run_id: str,
    settings: Optional[Settings] = None,
    max_pages: Optional[int] = None,
    max_listings: Optional[int] = None,
    checkpoint_dir: Optional[Path] = None,
) -> list[SearchCard]:
    """Discover listing URLs from all search pages across all provided search URLs.

    Args:
        search_urls: list of Portal Inmobiliario search page URLs
        run_id: unique ID for this scraper run
        settings: optional Settings override
        max_pages: max pages per search URL
        max_listings: overall listing cap
        checkpoint_dir: directory for checkpoint files (enables resume)

    Returns:
        Flat list of all discovered SearchCards (deduplicated by listing_id).
    """
    cfg = settings or get_settings()
    max_pages = max_pages or cfg.max_pages
    max_listings = max_listings or cfg.max_listings

    # Checkpoint: track which search URLs we've already fully paginated
    checkpoint_path = (
        (checkpoint_dir or cfg.output_dir / "checkpoints") / f"{run_id}_search_done.txt"
    )
    completed_search_urls = load_checkpoint(checkpoint_path)

    all_cards: list[SearchCard] = []
    seen_ids: set[str] = set()

    async with browser_session(cfg) as bm:
        service = PortalInmobiliarioService(bm, settings=cfg, run_id=run_id)

        for search_url in search_urls:
            if search_url in completed_search_urls:
                logger.info("search_url_already_done", url=search_url)
                continue

            logger.info("search_url_start", url=search_url)
            try:
                remaining = (max_listings - len(all_cards)) if max_listings else None
                cards = await service.fetch_all_search_pages(
                    start_url=search_url,
                    max_pages=max_pages,
                    max_listings=remaining,
                )
            except Exception as exc:
                logger.error("search_url_failed", url=search_url, error=str(exc), exc_info=True)
                continue

            # Deduplicate across search URLs
            new_cards = [c for c in cards if c.listing_id not in seen_ids]
            for card in new_cards:
                seen_ids.add(card.listing_id)

            all_cards.extend(new_cards)
            append_checkpoint(checkpoint_path, search_url)
            logger.info(
                "search_url_done",
                url=search_url,
                new_cards=len(new_cards),
                total=len(all_cards),
            )

            if max_listings and len(all_cards) >= max_listings:
                logger.info("global_max_listings_reached", limit=max_listings)
                break

    logger.info("search_pipeline_done", total_cards=len(all_cards))
    return all_cards
