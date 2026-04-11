"""
APScheduler-based cron scheduler for market-hours tasks.

All cron jobs run in IST (Asia/Kolkata).  Jobs are registered but only
fire when the Kite session is active.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings

logger = logging.getLogger("alpha_radar.scheduler")

_scheduler: Optional[AsyncIOScheduler] = None

IST = str(settings.TIMEZONE)  # "Asia/Kolkata"


def get_scheduler() -> AsyncIOScheduler:
    """Return (and lazily create) the singleton scheduler."""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone=IST)
    return _scheduler


def start_scheduler() -> None:
    """Start the scheduler if not already running."""
    sched = get_scheduler()
    if not sched.running:
        sched.start()
        logger.info("Scheduler started (%d jobs registered)", len(sched.get_jobs()))


def stop_scheduler() -> None:
    """Shutdown the scheduler gracefully."""
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def register_jobs(
    daily_reset: Callable,
    load_orb15: Callable,
    load_orb30: Callable,
    oi_snapshot: Callable,
    send_eod_report: Callable,
    save_session: Callable,
) -> None:
    """
    Register all market-hours cron jobs.

    Parameters are async callables (or sync -- APScheduler handles both).
    """
    sched = get_scheduler()

    # 08:30 IST -- clear caches, signals for a fresh trading day
    sched.add_job(
        daily_reset,
        CronTrigger(hour=8, minute=30, timezone=IST),
        id="daily_reset",
        name="Daily Reset (clear caches & signals)",
        replace_existing=True,
    )

    # 09:31 IST -- load 15-minute ORB (candle 09:15-09:30 is complete)
    sched.add_job(
        load_orb15,
        CronTrigger(hour=9, minute=31, timezone=IST),
        id="load_orb15",
        name="Load ORB-15",
        replace_existing=True,
    )

    # 09:46 IST -- load 30-minute ORB (candle 09:30-09:45 is complete)
    sched.add_job(
        load_orb30,
        CronTrigger(hour=9, minute=46, timezone=IST),
        id="load_orb30",
        name="Load ORB-30",
        replace_existing=True,
    )

    # Every 15 min from 09:55 to 15:30 -- OI snapshot
    sched.add_job(
        oi_snapshot,
        CronTrigger(
            hour="9-15",
            minute="10,25,40,55",
            timezone=IST,
        ),
        id="oi_snapshot",
        name="OI Snapshot (every 15 min)",
        replace_existing=True,
    )

    # 15:25 IST -- generate and send end-of-day report
    sched.add_job(
        send_eod_report,
        CronTrigger(hour=15, minute=25, timezone=IST),
        id="send_eod_report",
        name="Send EOD Report",
        replace_existing=True,
    )

    # 15:35 IST -- persist Kite session token for next-day restart
    sched.add_job(
        save_session,
        CronTrigger(hour=15, minute=35, timezone=IST),
        id="save_session",
        name="Save Session",
        replace_existing=True,
    )

    logger.info("Registered %d scheduled jobs", len(sched.get_jobs()))
