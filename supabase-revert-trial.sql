-- ============================================================================
-- Alpha Radar - Revert 14-day Trial (run when you want to remove the trial)
-- Run this in: Supabase Dashboard → SQL Editor → New Query → paste → Run
-- ============================================================================

-- Restore the original trigger: new signups get a free plan, not a trial.
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO public.subscriptions (user_id, plan, status)
  VALUES (NEW.id, 'free', 'active');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Optional: expire any still-active trials that were granted during the trial window.
-- Uncomment if you want to immediately end ongoing trials rather than let them run their 14 days.
--
-- UPDATE public.subscriptions
--    SET plan = 'free',
--        status = 'expired',
--        updated_at = NOW()
--  WHERE status = 'trialing'
--    AND razorpay_subscription_id IS NULL;
