"""Repository for Listing CRUD operations against SQLite."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import (
    ListingRow,
    ListingRunRow,
    ScraperRunRow,
    listing_to_row,
    row_to_dict,
)
from src.logging_config import get_logger
from src.models import Listing, ListingChange, ScraperRun

logger = get_logger(__name__)


class ListingRepository:
    """Async repository for all listing persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ── Listings ──────────────────────────────────────────────────────────────

    async def get_by_id(self, listing_id: str) -> Optional[ListingRow]:
        result = await self.session.execute(
            select(ListingRow).where(ListingRow.listing_id == listing_id)
        )
        return result.scalar_one_or_none()

    async def get_all_active(self) -> Sequence[ListingRow]:
        result = await self.session.execute(
            select(ListingRow).where(ListingRow.is_active == True)
        )
        return result.scalars().all()

    async def get_by_run(self, run_id: str) -> Sequence[ListingRunRow]:
        result = await self.session.execute(
            select(ListingRunRow).where(ListingRunRow.run_id == run_id)
        )
        return result.scalars().all()

    async def upsert(self, listing: Listing, run_id: Optional[str] = None) -> bool:
        """Insert or update a listing. Returns True if it was a new record."""
        existing = await self.get_by_id(listing.listing_id)
        is_new = existing is None

        now = datetime.utcnow()
        row_data = listing_to_row(listing)

        if is_new:
            row_data["first_seen_at"] = now
            row_data["last_seen_at"] = now
            row = ListingRow(**{k: v for k, v in row_data.items() if hasattr(ListingRow, k)})
            self.session.add(row)
            logger.debug("listing_insert", listing_id=listing.listing_id)
        else:
            row_data["last_seen_at"] = now
            row_data["first_seen_at"] = existing.first_seen_at or existing.scraped_at
            await self.session.execute(
                update(ListingRow)
                .where(ListingRow.listing_id == listing.listing_id)
                .values(**{k: v for k, v in row_data.items() if k != "listing_id" and hasattr(ListingRow, k)})
            )
            logger.debug("listing_update", listing_id=listing.listing_id)

        # Snapshot in listing_runs
        if run_id:
            await self._record_run_snapshot(listing, run_id)

        await self.session.flush()
        return is_new

    async def _record_run_snapshot(self, listing: Listing, run_id: str) -> None:
        snap = ListingRunRow(
            listing_id=listing.listing_id,
            run_id=run_id,
            scraped_at=listing.scraped_at or datetime.utcnow(),
            price=listing.price,
            currency=listing.currency,
            is_active=listing.is_active,
            fingerprint=listing.fingerprint,
        )
        self.session.add(snap)

    async def mark_inactive(self, listing_ids: list[str]) -> None:
        """Mark listings as inactive (disappeared from search results)."""
        if not listing_ids:
            return
        await self.session.execute(
            update(ListingRow)
            .where(ListingRow.listing_id.in_(listing_ids))
            .values(is_active=False, last_seen_at=datetime.utcnow())
        )
        logger.info("listings_marked_inactive", count=len(listing_ids))

    async def get_existing_ids(self) -> set[str]:
        """Return all known listing IDs (for deduplication)."""
        result = await self.session.execute(select(ListingRow.listing_id))
        return set(result.scalars().all())

    async def get_new_since(self, since: datetime) -> Sequence[ListingRow]:
        result = await self.session.execute(
            select(ListingRow).where(ListingRow.first_seen_at >= since)
        )
        return result.scalars().all()

    # ── Scraper runs ──────────────────────────────────────────────────────────

    async def create_run(self, run: ScraperRun) -> None:
        row = ScraperRunRow(
            run_id=run.run_id,
            started_at=run.started_at,
            status=run.status,
            search_urls=json.dumps(run.search_urls),
        )
        self.session.add(row)
        await self.session.flush()

    async def update_run(self, run: ScraperRun) -> None:
        await self.session.execute(
            update(ScraperRunRow)
            .where(ScraperRunRow.run_id == run.run_id)
            .values(
                finished_at=run.finished_at,
                status=run.status,
                number_discovered=run.number_discovered,
                number_scraped=run.number_scraped,
                number_new=run.number_new,
                number_changed=run.number_changed,
                number_failed=run.number_failed,
                error_message=run.error_message,
            )
        )

    async def get_last_run(self) -> Optional[ScraperRunRow]:
        result = await self.session.execute(
            select(ScraperRunRow)
            .where(ScraperRunRow.status.in_(["completed", "partial"]))
            .order_by(ScraperRunRow.finished_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
