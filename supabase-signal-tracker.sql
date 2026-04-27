-- ============================================================================
-- Alpha Radar — Signal Validation Tracker
-- Tables for the 15-day "shadow mode" experiment: log every signal fire and
-- measure outcomes at +15m / +1h / EOD / next-day-EOD.
--
-- Run in: Supabase Dashboard → SQL Editor → New Query → paste → Run
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 1. signal_fires — one row per signal that fires
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.signal_fires (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_type     TEXT        NOT NULL,
    -- 'OI_BULLISH' | 'OI_BEARISH' | 'PDH_CROSS' | 'COMBO_SURGE'
    -- | 'PDL_CROSS' | 'COMBO_SELL' | 'ORB_BREAK_15' | 'ORB_BREAK_30'
    -- | 'ORB_BREAKDOWN_15' | 'ORB_BREAKDOWN_30' | '52W_BOUNCE' | 'PULSE_BULL' | 'PULSE_BEAR'
  symbol          TEXT        NOT NULL,
  category        TEXT,                 -- 'index' | 'stock'
  fired_at        TIMESTAMPTZ NOT NULL,
  trigger_price   NUMERIC,              -- price at signal fire moment
  strength        NUMERIC,              -- engine-specific (-100..100 OI score, 0..100 buying score)
  direction       TEXT,                 -- 'BULLISH' | 'BEARISH' | 'NEUTRAL'
  confidence      TEXT,                 -- 'STRONG' | 'MODERATE' | 'WEAK'
  metadata        JSONB DEFAULT '{}'::jsonb,
    -- engine-specific extras: vol_ratio, pcr, oi_change, target, sl, etc.
  context         JSONB DEFAULT '{}'::jsonb,
    -- market context snapshot: { regime: 'BULL', advances, declines, vix, market_phase }
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2. signal_outcomes — one row per (fire × horizon)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.signal_outcomes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_fire_id  UUID NOT NULL REFERENCES public.signal_fires(id) ON DELETE CASCADE,
  horizon         TEXT NOT NULL,        -- '15m' | '1h' | 'eod' | 'next_day_eod'
  status          TEXT NOT NULL DEFAULT 'PENDING',
    -- 'PENDING' | 'WIN' | 'LOSS' | 'FLAT' | 'SKIPPED'
  entry_price     NUMERIC,              -- next-bar open after fire (realistic entry)
  exit_price      NUMERIC,              -- price at horizon
  high_during     NUMERIC,              -- max price between entry and horizon
  low_during      NUMERIC,              -- min price between entry and horizon
  return_pct      NUMERIC,              -- (exit - entry) / entry * 100, signed by direction
  mfe_pct         NUMERIC,              -- max favorable excursion
  mae_pct         NUMERIC,              -- max adverse excursion
  checked_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (signal_fire_id, horizon)
);

-- ────────────────────────────────────────────────────────────────────────────
-- 3. Indexes for hot lookups
-- ────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_signal_fires_fired_at
  ON public.signal_fires(fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_fires_symbol_type_time
  ON public.signal_fires(symbol, signal_type, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_fires_signal_type
  ON public.signal_fires(signal_type);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_status_horizon
  ON public.signal_outcomes(status, horizon);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_signal_fire
  ON public.signal_outcomes(signal_fire_id);

-- ────────────────────────────────────────────────────────────────────────────
-- 4. RLS — service role bypasses; no other access until admin UI is built
-- ────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.signal_fires    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.signal_outcomes ENABLE ROW LEVEL SECURITY;

-- Empty policy set — only service role (backend) can read/write during the experiment.
-- When we open this to admin UI we'll add: USING (auth.jwt() ->> 'role' = 'admin')
