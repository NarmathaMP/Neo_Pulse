<div align="center">
<img width="1200" height="475" alt="GHBanner" src="https://ai.google.dev/static/site-assets/images/share-ais-513315318.png" />
</div>

# Run and deploy your AI Studio app

This contains everything you need to run your app locally.

View your app in AI Studio: https://ai.studio/apps/a18b4365-0bff-4516-8b3a-fe694880623e

## Run Locally

**Prerequisites:**  Node.js


1. Install dependencies:
   `npm install`
2. Copy `.env.example` to `.env` and fill in:
   - `GEMINI_API_KEY` — your Gemini API key
   - `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` — see [Supabase Setup](#supabase-setup) below
3. Run the app:
   `npm run dev`

## Supabase Setup

NeoPulse persists patients, vitals, alerts, shift notes, and the nurse-feedback recalibration state to Supabase (PostgreSQL). Without it configured, the app still runs fine on an in-memory dataset — nothing is lost, it just won't survive a server restart.

1. Create a free project at [supabase.com](https://supabase.com).
2. In the Supabase dashboard, open **SQL Editor**, paste the contents of [`supabase/schema.sql`](supabase/schema.sql), and run it. This creates all 7 tables (`patients`, `vitals_readings`, `risk_scores`, `digital_twin_matches`, `alerts`, `shift_notes`, `unit_thresholds`).
3. Go to **Project Settings → API** and copy:
   - **Project URL** → `SUPABASE_URL`
   - **service_role** secret key → `SUPABASE_SERVICE_ROLE_KEY` (⚠️ never expose this key to the browser — it's used server-side only, which is exactly how this app uses it)
4. Paste both into your `.env` file.
5. Run `npm run dev` (or `npm run db:seed` to seed Supabase without starting the server).

**What happens on startup:**
- If Supabase already has patient rows → NeoPulse loads everything from Supabase (Supabase is now the source of truth).
- If Supabase is empty → NeoPulse builds its dataset from `data/cleaned_icu_dataset.csv` (3,196 cleaned ICU patient records) and pushes it into Supabase automatically.
- Every new vital reading, alert, shift note, patient admission, and nurse feedback event is written through to Supabase in real time as the app runs.
- If `SUPABASE_URL`/`SUPABASE_SERVICE_ROLE_KEY` aren't set at all, NeoPulse just runs on the in-memory dataset, same as before.


## Importing your own data

Click **Import Data** on the dashboard (or `POST /api/datasets/upload?mode=append|replace&limit=100`, body = raw file text).

- Accepts CSV, TSV, semicolon-CSV and JSON. Column names are matched case-insensitively with aliases
  (`HR` = `heart rate` = `pulse`, `SpO2` = `SaO2` = `o2sat`, `RR` = `RespRate`, `MAP` = `mean bp`, ...).
- Layouts: PhysioNet-style aggregate columns (`HR_first`, `HR_last`, `HR_mean` ...), one row per patient with plain
  vital columns, or time-series (wide: `patient_id, timestamp, hr, spo2 ...`; long: `patient_id, timestamp, metric, value`).
- Missing values (`""`, `NA`, `null`, `-1`) are fine. Physiologically impossible values are treated as missing using the same
  ranges as `cleaning_log_v2`. Fahrenheit temperatures are converted to Celsius.
- Real timestamps are kept (rebased so the newest reading is "now"). Aggregate-only rows get a trajectory generated from
  first/last/min/max/mean; metrics with no data at all are generated from baseline and flagged `is_imputed`.
- A file with nothing mappable (no recognisable columns) is the only thing rejected, with a message saying why.
- Code: `src/server/services/ingest.ts` (parser), `csvLoader.ts` (default file goes through the same parser),
  `database.ts -> importRecords()` (adds patients/vitals, persists to Supabase).

## Alert system

Implemented in `src/server/services/alertEngine.ts`; evaluated after every new reading, after every import/admission and
every 30 s in the background.

- **Threshold rules**: HR > 120 / > 140 / < 50 / < 40, SpO2 < 90 / < 85, MAP < 65 / < 55, RR > 28 / > 35 / < 8 / < 6.
- **Trend rules** (1 h): MAP falls 20, HR rises 30, SpO2 falls 5, RR rises 10.
- **Model rule**: GRU-D risk model reaches concern / critical.
- De-duplicated per patient+rule (15 min cooldown), auto-resolved when the value recovers.
- UI: bell in the header (`components/AlertCenter.tsx`) with active alerts, acknowledge, rule list, toast popups and a beep
  for new alerts (mute toggle); the dashboard banner shows active alerts, most severe first.
- API: `GET /api/alerts`, `GET /api/alerts/rules`, `POST /api/alerts/evaluate`, `POST /api/alerts/:id/acknowledge`,
  SSE events `alert_raised` / `alert_resolved`.
