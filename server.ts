import express, { Request, Response } from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { createServer as createViteServer } from 'vite';
import { clinicalDb } from './src/server/database.ts';
import { runGRUDInference } from './src/server/models/grud.ts';
import { matchDigitalTwins } from './src/server/models/digitalTwin.ts';
import { generateClinicalNarrative, generateShiftHandoffNote } from './src/server/services/narrative.ts';
import { VitalMetric, SCHEMA_SQL } from './src/types/clinical.ts';
import { isSupabaseConnected } from './src/server/supabaseClient.ts';
import { parseUpload, resolveMetric, implausibleReason } from './src/server/services/ingest.ts';
import { evaluatePatient, evaluatePatients, sweepAll, describeRules, ALERT_SWEEP_MS } from './src/server/services/alertEngine.ts';
import { classifyHistoricalRecord } from './src/server/services/historicalClassification.ts';
import type { AlertEvents } from './src/server/services/alertEngine.ts';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT) || 3000;

// -------------------------------------------------------------
// Dataset upload — accepts ANY csv / tsv / json layout (see services/ingest.ts).
// Registered before express.json() so big files are read as raw text (client sends text/plain).
//   POST /api/datasets/upload?mode=append|replace&limit=100&filename=x.csv
// -------------------------------------------------------------
app.post('/api/datasets/upload', express.text({ type: () => true, limit: '200mb' }), async (req: Request, res: Response) => {
  try {
    const body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body ?? '');
    const mode = req.query.mode === 'replace' ? 'replace' : 'append';
    const limit = Math.max(1, Math.min(1000, Number(req.query.limit) || 100));
    const filename = String(req.query.filename || '');

    const { records, report } = parseUpload(body, filename);
    if (records.length === 0) {
      return res.status(422).json({ success: false, report, error: report.warnings[0] || 'No usable patient rows were found in this file.' });
    }

    const result = await clinicalDb.importRecords(records, { mode, limit });
    if (result.truncated > 0) {
      report.warnings.push(`${result.truncated} more patient(s) were not loaded because the limit is ${limit} beds — raise the limit or upload a smaller file.`);
    }
    if (result.skippedDuplicates > 0) {
      report.warnings.push(`${result.skippedDuplicates} patient(s) were skipped because their ID is already loaded (use "Replace" to overwrite).`);
    }
    if (isSupabaseConnected() && !result.persisted) {
      report.warnings.push('Supabase save failed — the data is live in memory but will be lost on restart. Check the server log.');
    }

    result.events.raised.forEach(a => broadcastSSE('alert_raised', a));
    result.events.resolved.forEach(a => broadcastSSE('alert_resolved', a));
    broadcastSSE('dataset_imported', { patients: result.patients.length, mode });

    res.json({
      success: true,
      mode,
      report,
      imported: {
        patients: result.patients.length,
        vital_readings: result.vitals.length,
        skipped_duplicates: result.skippedDuplicates,
        not_loaded_over_limit: result.truncated,
        alerts_raised: result.events.raised.length,
        saved_to_supabase: result.persisted,
      },
      totals: { patients: clinicalDb.patients.length, vitals: clinicalDb.vitals.length, alerts: clinicalDb.alerts.length },
    });
  } catch (err: any) {
    console.error('[Upload] failed:', err);
    res.status(500).json({ success: false, error: err.message || 'Upload failed' });
  }
});

app.use(express.json());

// SSE Clients for live monitoring stream
interface SSEClient {
  id: number;
  res: Response;
}
let sseClients: SSEClient[] = [];
let nextClientId = 1;

function broadcastSSE(event: string, data: any) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  sseClients.forEach(c => {
    try {
      c.res.write(payload);
    } catch {
      // client dropped
    }
  });
}

// Runs the alert engine and pushes raised / resolved alerts to every connected browser (SSE).
function publishAlertEvents(events: AlertEvents) {
  events.raised.forEach(a => broadcastSSE('alert_raised', a));
  events.resolved.forEach(a => broadcastSSE('alert_resolved', a));
}

// -------------------------------------------------------------
// API Endpoints (as defined in the NeoPulse Architecture Spec)
// -------------------------------------------------------------

// 1. GET /api/patients - List all patients with their current tier and latest vitals
app.get('/api/patients', (req: Request, res: Response) => {
  try {
    const list = clinicalDb.patients.map(p => {
      const risk = clinicalDb.getCurrentRisk(p.patient_id);
      const readings = clinicalDb.getVitalsForPatient(p.patient_id);
      const latestMetrics: Partial<Record<VitalMetric, { value: number; recorded_at: string; delta_t: number }>> = {};

      const metrics: VitalMetric[] = ['heart_rate', 'spo2', 'resp_rate', 'map'];
      for (const reading of readings) {
        if (!metrics.includes(reading.metric)) continue;
        const current = latestMetrics[reading.metric];
        if (!current || reading.recorded_at > current.recorded_at) {
          latestMetrics[reading.metric] = {
            value: reading.value,
            recorded_at: reading.recorded_at,
            delta_t: reading.delta_t_seconds
          };
        }
      }

      if (p.source_data_type === 'aggregate' && p.vital_summary) {
        const sourceMetrics: Array<[VitalMetric, string[]]> = [
          ['heart_rate', ['HR']],
          ['spo2', ['SaO2']],
          ['resp_rate', ['RespRate']],
          ['map', ['MAP', 'NIMAP']],
        ];
        for (const [metric, keys] of sourceMetrics) {
          const summary = keys.map(key => p.vital_summary?.[key]).find(value => value?.count);
          if (summary?.mean !== null && summary?.mean !== undefined) {
            latestMetrics[metric] = { value: Number(summary.mean.toFixed(1)), recorded_at: '', delta_t: 0 };
          }
        }
      }

      const { labs: _labs, ...patientSummary } = p;

      return {
        ...patientSummary,
        historical_classification: p.source_data_type === 'aggregate' ? classifyHistoricalRecord(p) : undefined,
        current_risk: risk.risk_score,
        current_tier: risk.tier,
        confidence: risk.confidence,
        sampling_rate_per_hour: risk.sampling_rate_per_hour,
        latest_metrics: latestMetrics
      };
    });

    res.json({ patients: list, unit_thresholds: clinicalDb.unitThresholds });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 2. GET /api/patients/:id - Patient metadata
app.get('/api/patients/:id', (req: Request, res: Response) => {
  const patient = clinicalDb.patients.find(p => p.patient_id === req.params.id);
  if (!patient) {
    return res.status(404).json({ error: 'Patient not found' });
  }

  const risk = clinicalDb.getCurrentRisk(patient.patient_id);
  res.json({
    patient: {
      ...patient,
      historical_classification: patient.source_data_type === 'aggregate' ? classifyHistoricalRecord(patient) : undefined,
    },
    risk,
  });
});

// 3. POST /api/patients - Admit new patient
app.post('/api/patients', (req: Request, res: Response) => {
  const { name, age, sex, bed_number, admission_diagnosis, unit, height_cm, weight_kg, mech_vent_ever, mech_vent_hours } = req.body;
  if (!name || !age || !admission_diagnosis) {
    return res.status(400).json({ error: 'Missing required patient fields' });
  }

  const newPatient = {
    patient_id: `pat-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
    bed_number: bed_number || 'Bed 01-A',
    name,
    admitted_at: new Date().toISOString(),
    age: Number(age),
    sex: (sex === 'F' ? 'F' : 'M') as 'M' | 'F',
    admission_diagnosis,
    unit: unit || 'Medical ICU (MICU)',
    height_cm: height_cm ? Number(height_cm) : null,
    weight_kg: weight_kg ? Number(weight_kg) : null,
    mech_vent_ever: (mech_vent_ever ? 1 : 0) as 0 | 1,
    mech_vent_hours: mech_vent_hours ? Number(mech_vent_hours) : 0,
  };

  clinicalDb.admitPatient(newPatient); // updates in-memory state + persists to Supabase

  // Ingest initial baseline reading
  clinicalDb.ingestReading(newPatient.patient_id, 'heart_rate', 76);
  clinicalDb.ingestReading(newPatient.patient_id, 'spo2', 98);
  clinicalDb.ingestReading(newPatient.patient_id, 'resp_rate', 16);
  clinicalDb.ingestReading(newPatient.patient_id, 'map', 84);

  publishAlertEvents(evaluatePatient(clinicalDb, newPatient.patient_id));
  broadcastSSE('patient_admitted', newPatient);
  res.status(201).json(newPatient);
});

// 4. POST /api/patients/:id/ingest - Push new vitals reading
app.post('/api/patients/:id/ingest', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const { metric: rawMetric, value: rawValue, recorded_at, source } = req.body;

  if (!rawMetric || rawValue === undefined || rawValue === null || rawValue === '') {
    return res.status(400).json({ error: 'Metric and value are required' });
  }

  if (!clinicalDb.patients.some(p => p.patient_id === patientId)) {
    return res.status(404).json({ error: 'Patient not found' });
  }

  // Accept any reasonable metric name: heart_rate / HR / pulse, spo2 / SaO2 / o2sat, resp_rate / RR, map / mean bp ...
  const metric = resolveMetric(String(rawMetric));
  if (!metric) {
    return res.status(400).json({ error: `Unknown metric "${rawMetric}". Use heart_rate (HR), spo2, resp_rate (RR) or map.` });
  }
  const cleaned = typeof rawValue === 'number' ? String(rawValue) : String(rawValue).replace(/[^\d.eE+-]/g, '');
  const value = /\d/.test(cleaned) ? Number(cleaned) : NaN;
  if (!Number.isFinite(value)) {
    return res.status(400).json({ error: `Value "${rawValue}" is not a number` });
  }
  const impossible = implausibleReason(metric, value);
  if (impossible) {
    return res.status(422).json({ error: `Rejected as a sensor artifact: ${impossible}.` });
  }
  let recordedAtIso: string | undefined = undefined;
  if (recorded_at) {
    const d = new Date(recorded_at);
    if (Number.isNaN(d.getTime())) return res.status(400).json({ error: `recorded_at "${recorded_at}" is not a valid date` });
    recordedAtIso = d.toISOString();
  }

  const reading = clinicalDb.ingestReading(patientId, metric, value, recordedAtIso, source);
  const updatedRisk = clinicalDb.getCurrentRisk(patientId);

  // Alert engine: threshold + trend + model-tier rules (see src/server/services/alertEngine.ts)
  publishAlertEvents(evaluatePatient(clinicalDb, patientId));

  broadcastSSE('vital_ingested', { patient_id: patientId, reading, risk: updatedRisk });

  res.json({
    success: true,
    reading,
    risk: updatedRisk
  });
});

// 5. GET /api/patients/:id/trajectory - Full time-series + risk history
app.get('/api/patients/:id/trajectory', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const t = req.query.t as string | undefined;

  const rawReadings = clinicalDb.getPatientTrajectory(patientId, t);
  const patient = clinicalDb.patients.find(p => p.patient_id === patientId);

  if (!patient) {
    return res.status(404).json({ error: 'Patient not found' });
  }

  if (patient.source_data_type === 'aggregate') {
    return res.json({ patient_id: patientId, source: 'aggregate', total_readings: 0, trajectory: [] });
  }

  // Group readings by timestamp to form synchronized trajectory chart points
  const timeMap = new Map<string, any>();

  rawReadings.forEach(r => {
    // Round to nearest 15 minutes for clean time alignment
    const dateObj = new Date(r.recorded_at);
    const roundedTime = new Date(Math.round(dateObj.getTime() / (15 * 60 * 1000)) * (15 * 60 * 1000)).toISOString();

    if (!timeMap.has(roundedTime)) {
      const admittedTime = new Date(patient.admitted_at).getTime();
      const pointTime = new Date(roundedTime).getTime();
      const relativeHours = Number(((pointTime - admittedTime) / (3600 * 1000)).toFixed(1));

      timeMap.set(roundedTime, {
        timestamp: roundedTime,
        relative_hour: relativeHours,
        exact_time: r.recorded_at,
        delta_t_seconds: {}
      });
    }

    const point = timeMap.get(roundedTime);
    point[r.metric] = r.value;
    point.delta_t_seconds[r.metric] = r.delta_t_seconds;
  });

  const trajectoryPoints = Array.from(timeMap.values()).sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  // Compute intermediate risk score progression at key steps
  let lastHr = 78, lastMap = 82, lastSpo2 = 98, lastRr = 16;
  const enrichedPoints = trajectoryPoints.map((pt, idx) => {
    if (pt.heart_rate) lastHr = pt.heart_rate;
    if (pt.map) lastMap = pt.map;
    if (pt.spo2) lastSpo2 = pt.spo2;
    if (pt.resp_rate) lastRr = pt.resp_rate;

    // Approximate step risk calculation
    const progressRisk = runGRUDInference(rawReadings, pt.timestamp, {
      watch: clinicalDb.unitThresholds.watch_threshold,
      concern: clinicalDb.unitThresholds.concern_threshold,
      critical: clinicalDb.unitThresholds.critical_threshold
    });

    return {
      ...pt,
      heart_rate: pt.heart_rate ?? lastHr,
      map: pt.map ?? lastMap,
      spo2: pt.spo2 ?? lastSpo2,
      resp_rate: pt.resp_rate ?? lastRr,
      risk_score: progressRisk.risk_score,
      tier: progressRisk.tier,
      sampling_rate: progressRisk.sampling_rate_per_hour
    };
  });

  res.json({
    patient_id: patientId,
    total_readings: rawReadings.length,
    trajectory: enrichedPoints
  });
});

// 6. GET /api/patients/:id/risk/current - Latest score, tier, confidence, feature attribution
app.get('/api/patients/:id/risk/current', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const t = req.query.t as string | undefined;

  const risk = clinicalDb.getCurrentRisk(patientId, t);
  res.json(risk);
});

// 7. GET /api/patients/:id/narrative - Latest LLM explanation (Gemini API with fallback)
app.get('/api/patients/:id/narrative', async (req: Request, res: Response) => {
  try {
    const patientId = req.params.id;
    const t = req.query.t as string | undefined;
    const patient = clinicalDb.patients.find(p => p.patient_id === patientId);

    if (!patient) {
      return res.status(404).json({ error: 'Patient not found' });
    }

    const readings = clinicalDb.getVitalsForPatient(patientId);
    const risk = clinicalDb.getCurrentRisk(patientId, t);
    const twins = matchDigitalTwins(patientId, readings, t);

    const narrativeResult = await generateClinicalNarrative(patient, risk, twins);

    res.json({
      patient_id: patientId,
      computed_at: t || new Date().toISOString(),
      tier: risk.tier,
      risk_score: risk.risk_score,
      ...narrativeResult
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 8. GET /api/patients/:id/digital-twins - Nearest-neighbor case matches
app.get('/api/patients/:id/digital-twins', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const t = req.query.t as string | undefined;

  const readings = clinicalDb.getVitalsForPatient(patientId);
  const twins = matchDigitalTwins(patientId, readings, t);

  res.json(twins);
});

// 9. POST /api/patients/:id/feedback - Nurse feedback recalibration loop
app.post('/api/patients/:id/feedback', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const { alert_id, feedback, reason } = req.body;

  if (!alert_id || !feedback || !['true_positive', 'false_alarm'].includes(feedback)) {
    return res.status(400).json({ error: 'Valid alert_id and feedback (true_positive | false_alarm) required' });
  }

  const result = clinicalDb.recordFeedback(patientId, alert_id, feedback, reason);
  broadcastSSE('recalibration_updated', result.unitThresholds);

  res.json({
    success: true,
    ...result
  });
});

// 10. GET /api/patients/:id/shift-note - Auto-generated SBAR handoff summary
app.get('/api/patients/:id/shift-note', async (req: Request, res: Response) => {
  try {
    const patientId = req.params.id;
    const patient = clinicalDb.patients.find(p => p.patient_id === patientId);

    if (!patient) {
      return res.status(404).json({ error: 'Patient not found' });
    }

    const readings = clinicalDb.getVitalsForPatient(patientId);
    const risk = clinicalDb.getCurrentRisk(patientId);
    const twins = matchDigitalTwins(patientId, readings);

    const shiftNote = await generateShiftHandoffNote(patient, risk, twins);
    clinicalDb.addShiftNote(shiftNote); // updates in-memory state + persists to Supabase

    res.json(shiftNote);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// 11. GET /api/patients/:id/replay?t=... - Counterfactual replay slider (no future peeking)
app.get('/api/patients/:id/replay', (req: Request, res: Response) => {
  const patientId = req.params.id;
  const t = req.query.t as string;

  if (!t) {
    return res.status(400).json({ error: 'Timestamp query param t is required' });
  }

  const patient = clinicalDb.patients.find(p => p.patient_id === patientId);
  if (!patient) {
    return res.status(404).json({ error: 'Patient not found' });
  }

  const readings = clinicalDb.getPatientTrajectory(patientId, t);
  const rawRisk = runGRUDInference(readings, t, {
    watch: clinicalDb.unitThresholds.watch_threshold + clinicalDb.unitThresholds.recalibration_offset,
    concern: clinicalDb.unitThresholds.concern_threshold + clinicalDb.unitThresholds.recalibration_offset,
    critical: clinicalDb.unitThresholds.critical_threshold + clinicalDb.unitThresholds.recalibration_offset
  });
  const risk = {
    score_id: Date.now(),
    patient_id: patientId,
    computed_at: t,
    ...rawRisk
  };
  const twins = matchDigitalTwins(patientId, readings, t);

  res.json({
    replay_timestamp: t,
    patient_id: patientId,
    readings_available_at_t: readings.length,
    no_future_peeking_verified: true,
    risk,
    twins
  });
});

// 12. GET /api/alerts - List all alerts (newest first, with patient name / bed for the UI)
app.get('/api/alerts', (req: Request, res: Response) => {
  const byId = new Map(clinicalDb.patients.map(p => [p.patient_id, p]));
  const alerts = [...clinicalDb.alerts]
    .sort((a, b) => new Date(b.raised_at).getTime() - new Date(a.raised_at).getTime())
    .map(a => ({ ...a, patient_name: byId.get(a.patient_id)?.name, bed_number: byId.get(a.patient_id)?.bed_number }));
  res.json({ alerts });
});

// 12b. GET /api/alerts/rules - the rules the alert engine evaluates
app.get('/api/alerts/rules', (req: Request, res: Response) => {
  res.json(describeRules());
});

// 12c. POST /api/alerts/evaluate - run the alert engine now for every patient
app.post('/api/alerts/evaluate', (req: Request, res: Response) => {
  const events = sweepAll(clinicalDb);
  publishAlertEvents(events);
  res.json({ raised: events.raised.length, resolved: events.resolved.length });
});

// 13. POST /api/alerts/:id/acknowledge
app.post('/api/alerts/:id/acknowledge', (req: Request, res: Response) => {
  const { nurse_name } = req.body;
  const alert = clinicalDb.acknowledgeAlert(req.params.id, nurse_name || 'Staff Nurse, RN');
  if (!alert) {
    return res.status(404).json({ error: 'Alert not found' });
  }
  broadcastSSE('alert_acknowledged', alert);
  res.json({ success: true, alert });
});

// 13b. GET /api/datasets/info - what is loaded right now
app.get('/api/datasets/info', (req: Request, res: Response) => {
  const sources = new Map<string, number>();
  clinicalDb.vitals.forEach(v => sources.set(v.source, (sources.get(v.source) || 0) + 1));
  res.json({
    patients: clinicalDb.patients.length,
    vital_readings: clinicalDb.vitals.length,
    vitals_by_source: Object.fromEntries(sources),
    supabase_connected: isSupabaseConnected(),
  });
});

// 14. GET /api/schema - Expose the PostgreSQL database schema
app.get('/api/schema', (req: Request, res: Response) => {
  res.json({
    engine: 'Supabase (PostgreSQL)',
    connected: isSupabaseConnected(),
    schema_sql: SCHEMA_SQL,
    tables: ['patients', 'vitals_readings', 'risk_scores', 'digital_twin_matches', 'alerts', 'shift_notes', 'unit_thresholds'],
    total_records: {
      patients: clinicalDb.patients.length,
      vitals_readings: clinicalDb.vitals.length,
      alerts: clinicalDb.alerts.length,
      shift_notes: clinicalDb.shiftNotes.length
    }
  });
});

// 15. Server-Sent Events / Live Telemetry Stream
app.get('/api/monitor/stream', (req: Request, res: Response) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const clientId = nextClientId++;
  sseClients.push({ id: clientId, res });

  // Send initial ping
  res.write(`event: connected\ndata: ${JSON.stringify({ clientId, timestamp: new Date().toISOString() })}\n\n`);

  req.on('close', () => {
    sseClients = sseClients.filter(c => c.id !== clientId);
  });
});

// -------------------------------------------------------------
// Vite Middleware / Static Serve integration
// -------------------------------------------------------------
async function startServer() {
  // Connect to Supabase (loads persisted data if present, otherwise seeds
  // Supabase from the in-memory ICU dataset). No-ops gracefully if
  // SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY aren't set in .env.
  await clinicalDb.initFromSupabase();

  // Evaluate the initial cohort in small batches so large datasets don't block
  // dashboard and patient-detail API requests while alerts are refreshed.
  let sweepRunning = false;
  const startAlertSweep = () => {
    if (sweepRunning) return;
    sweepRunning = true;
    let offset = 0;
    const batchSize = 25;
    const runBatch = () => {
      try {
        const ids = clinicalDb.patients.filter(patient => patient.source_data_type !== 'aggregate')
          .slice(offset, offset + batchSize).map(patient => patient.patient_id);
        if (ids.length === 0) {
          sweepRunning = false;
          return;
        }
        publishAlertEvents(evaluatePatients(clinicalDb, ids));
        offset += ids.length;
        setTimeout(runBatch, 10);
      } catch (error) {
        sweepRunning = false;
        console.error('[AlertEngine] sweep failed:', error);
      }
    };
    runBatch();
  };
  setTimeout(startAlertSweep, 1000);
  setInterval(startAlertSweep, ALERT_SWEEP_MS);

  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      // This app hosts Vite through Express; disable Vite's separate HMR
      // socket to avoid conflicts with other local Vite instances.
      server: { middlewareMode: true, hmr: false, ws: false },
      appType: 'spa'
    });
    app.use(vite.middlewares);

    app.use('*', async (req: Request, res: Response, next) => {
      const url = req.originalUrl;
      if (url.startsWith('/api')) {
        return next();
      }
      try {
        let template = fs.readFileSync(path.resolve(__dirname, 'index.html'), 'utf-8');
        template = await vite.transformIndexHtml(url, template);
        res.status(200).set({ 'Content-Type': 'text/html' }).end(template);
      } catch (e) {
        next(e);
      }
    });
  } else {
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (req: Request, res: Response) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  }

  const listen = (port: number) => {
    const server = app.listen(port, '0.0.0.0', () => {
      const address = server.address();
      const activePort = typeof address === 'object' && address ? address.port : port;
      console.log(`NeoPulse Clinical Intelligence Server running on http://localhost:${activePort}`);
    });
    server.once('error', (error: NodeJS.ErrnoException) => {
      if (error.code === 'EADDRINUSE') {
        console.warn(`[Server] Port ${port} is in use; trying ${port + 1}.`);
        listen(port + 1);
        return;
      }
      console.error('[Server] Failed to start:', error);
      process.exitCode = 1;
    });
  };
  listen(PORT);
}

startServer().catch(err => {
  console.error('Failed to start server:', err);
});
