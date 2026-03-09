"""Async Playwright browser management with retries, delays, and screenshot support."""
from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PWTimeoutError,
    async_playwright,
)
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import Settings, get_settings
from src.logging_config import get_logger

logger = get_logger(__name__)


class BrowserManager:
    """Manages a single Playwright browser instance with automatic restart on failure.

    Usage::

        async with BrowserManager(settings) as bm:
            async with bm.new_page() as page:
                await page.goto(url)
                html = await page.content()
    """

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._failure_count: int = 0
        self._max_failures_before_restart: int = 3

    async def start(self) -> None:
        self._playwright = await async_playwright().start()
        await self._launch_browser()

    async def stop(self) -> None:
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as exc:
            logger.warning("browser_stop_error", error=str(exc))
        finally:
            self._context = None
            self._browser = None
            self._playwright = None

    async def _launch_browser(self) -> None:
        assert self._playwright is not None
        logger.info("browser_launch", headless=self.settings.headless)
        self._browser = await self._playwright.chromium.launch(
            headless=self.settings.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=self.settings.user_agent,
            locale="es-CL",
            timezone_id="America/Santiago",
            accept_downloads=False,
        )
        # Mask webdriver flag to reduce bot detection
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self._failure_count = 0

    async def _restart_browser(self) -> None:
        logger.warning("browser_restart")
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        await self._launch_browser()

    @asynccontextmanager
    async def new_page(self) -> AsyncGenerator[Page, None]:
        """Yield a fresh Page; close it automatically on exit."""
        if self._context is None:
            await self._launch_browser()
        assert self._context is not None
        page = await self._context.new_page()
        try:
            yield page
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def fetch_page(
        self,
        url: str,
        *,
        wait_selector: Optional[str] = None,
        save_screenshot_on_error: bool = True,
        screenshot_dir: Optional[Path] = None,
    ) -> str:
        """Navigate to `url` and return page HTML content.

        Implements:
        - Exponential backoff retry on timeouts / errors
        - Human-like random delay before navigation
        - Screenshot capture on critical failures
        - Browser restart after repeated failures
        """
        settings = self.settings

        async def _attempt() -> str:
            # Random delay before each request
            delay = random.uniform(settings.min_delay, settings.max_delay)
            await asyncio.sleep(delay)

            async with self.new_page() as page:
                try:
                    logger.debug("page_navigate", url=url)
                    response = await page.goto(
                        url,
                        timeout=settings.browser_timeout,
                        wait_until="domcontentloaded",
                    )

                    # Wait for bot-challenge to resolve (Portal PI runs a proof-of-work)
                    # We wait for either a known content selector OR a generous timeout
                    try:
                        if wait_selector:
                            await page.wait_for_selector(
                                wait_selector,
                                timeout=settings.browser_timeout,
                            )
                        else:
                            # Generic: wait until JS settles
                            await page.wait_for_load_state("networkidle", timeout=15_000)
                    except PWTimeoutError:
                        # Not a fatal error — content may have loaded partially
                        logger.debug("wait_selector_timeout", url=url)

                    html = await page.content()

                    if response and response.status >= 400:
                        raise RuntimeError(f"HTTP {response.status} for {url}")

                    self._failure_count = 0
                    return html

                except PWTimeoutError as exc:
                    self._failure_count += 1
                    logger.warning(
                        "page_timeout",
                        url=url,
                        failures=self._failure_count,
                    )
                    if save_screenshot_on_error and screenshot_dir:
                        await _take_screenshot(page, url, screenshot_dir)
                    if self._failure_count >= self._max_failures_before_restart:
                        await self._restart_browser()
                    raise exc

                except Exception as exc:
                    self._failure_count += 1
                    logger.warning(
                        "page_error",
                        url=url,
                        error=str(exc),
                        failures=self._failure_count,
                    )
                    if self._failure_count >= self._max_failures_before_restart:
                        await self._restart_browser()
                    raise

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(settings.max_retries),
                wait=wait_exponential(
                    multiplier=settings.retry_backoff, min=1, max=30
                ),
                retry=retry_if_exception_type((PWTimeoutError, RuntimeError, Exception)),
                reraise=True,
            ):
                with attempt:
                    return await _attempt()
        except RetryError as exc:
            logger.error("page_fetch_exhausted", url=url, error=str(exc))
            raise RuntimeError(f"Failed to fetch {url} after {settings.max_retries} retries") from exc

        # Should never reach here
        raise RuntimeError(f"Unreachable: fetch_page({url})")  # pragma: no cover


async def _take_screenshot(page: Page, url: str, screenshot_dir: Path) -> None:
    from src.utils import slugify, url_to_id
    from datetime import datetime

    try:
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        name = slugify(url_to_id(url) or url[:40])
        path = screenshot_dir / f"error_{ts}_{name}.png"
        await page.screenshot(path=str(path), full_page=False)
        logger.info("screenshot_saved", path=str(path))
    except Exception as exc:
        logger.warning("screenshot_failed", error=str(exc))


@asynccontextmanager
async def browser_session(
    settings: Optional[Settings] = None,
) -> AsyncGenerator[BrowserManager, None]:
    """Context manager that starts and stops a BrowserManager."""
    bm = BrowserManager(settings)
    await bm.start()
    try:
        yield bm
    finally:
        await bm.stop()
