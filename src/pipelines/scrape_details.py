"""Pipeline: visit detail pages and persist full Listing records."""
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.browser import browser_session
from src.config import Settings, get_settings
from src.database import get_session_factory
from src.dedupe import compute_fingerprint, detect_changes, merge_listing
from src.logging_config import get_logger
from src.models import Listing, ScraperRun, SearchCard
from src.repositories.listing_repository import ListingRepository
from src.services.portal_inmobiliario import PortalInmobiliarioService
from src.utils import append_checkpoint, load_checkpoint

logger = get_logger(__name__)


async def run_details_pipeline(
    cards: list[SearchCard],
    run_id: str,
    scraper_run: ScraperRun,
    settings: Optional[Settings] = None,
    checkpoint_dir: Optional[Path] = None,
) -> ScraperRun:
    """Fetch and persist detail pages for the given SearchCards.

    Mutates and returns `scraper_run` with updated counters.
    """
    cfg = settings or get_settings()
    session_factory = get_session_factory(cfg.database_url)

    checkpoint_path = (
        (checkpoint_dir or cfg.output_dir / "checkpoints")
        / f"{run_id}_details_done.txt"
    )
    done_urls = load_checkpoint(checkpoint_path)

    pending = [c for c in cards if c.listing_url not in done_urls]
    logger.info(
        "details_pipeline_start",
        total=len(cards),
        pending=len(pending),
        already_done=len(done_urls),
    )

    async with browser_session(cfg) as bm:
        service = PortalInmobiliarioService(bm, settings=cfg, run_id=run_id)

        for card in pending:
            listing: Optional[Listing] = None
            try:
                listing = await service.fetch_detail_page(card)
            except Exception as exc:
                logger.error(
                    "detail_fetch_error",
                    url=card.listing_url,
                    error=str(exc),
                )
                scraper_run.number_failed += 1
                continue

            if listing is None:
                scraper_run.number_failed += 1
                continue

            # Fingerprint for dedup / change detection
            compute_fingerprint(listing)

            # Persist
            try:
                async with session_factory() as session:
                    repo = ListingRepository(session)
                    existing = await repo.get_by_id(listing.listing_id)

                    if existing is None:
                        is_new = await repo.upsert(listing, run_id=run_id)
                        scraper_run.number_new += 1
                    else:
                        # Convert existing row to Listing for comparison
                        from src.database import row_to_dict
                        from src.models import Listing as ListingModel

                        existing_listing = _row_to_listing(existing)
                        changes = detect_changes(listing, existing_listing, run_id=run_id)

                        if changes:
                            scraper_run.number_changed += 1
                            for change in changes:
                                logger.info(
                                    "field_changed",
                                    listing_id=listing.listing_id,
                                    field=change.field_name,
                                    old=change.old_value,
                                    new=change.new_value,
                                )

                        merged = merge_listing(listing, existing_listing)
                        await repo.upsert(merged, run_id=run_id)

                    await repo.commit()
                    scraper_run.number_scraped += 1
                    append_checkpoint(checkpoint_path, card.listing_url)

            except Exception as exc:
                logger.error(
                    "detail_persist_error",
                    listing_id=listing.listing_id if listing else "?",
                    error=str(exc),
                )
                scraper_run.number_failed += 1

    logger.info(
        "details_pipeline_done",
        scraped=scraper_run.number_scraped,
        new=scraper_run.number_new,
        changed=scraper_run.number_changed,
        failed=scraper_run.number_failed,
    )
    return scraper_run


def _row_to_listing(row) -> Listing:
    """Convert a ListingRow ORM object to a Listing pydantic model (best-effort)."""
    from src.database import row_to_dict

    d = row_to_dict(row)
    # Pydantic will coerce types; ignore unknown fields
    try:
        return Listing.model_validate(d)
    except Exception:
        # Fallback: construct with minimal fields
        return Listing(
            listing_id=row.listing_id,
            listing_url=row.listing_url,
            price=row.price,
            currency=row.currency,
            bedrooms=row.bedrooms,
            bathrooms=row.bathrooms,
            usable_area_m2=row.usable_area_m2,
            commune=row.commune,
            is_active=row.is_active,
            fingerprint=row.fingerprint,
            scraped_at=row.scraped_at or datetime.utcnow(),
        )
