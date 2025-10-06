import { getAdminClient } from '../shared/supabaseClient.js';
import { sleep, loopMs } from '../shared/utils.js';

const LOOP_MS = loopMs('EXPORT_LOOP_MS', 15000);

async function runOne(supabase) {
  const { data: jobs, error } = await supabase
    .from('export_job')
    .select('*')
    .eq('status', 'queued')
    .order('created_at', { ascending: true })
    .limit(1);

  if (error) throw error;
  const job = (jobs || [])[0];
  if (!job) return;

  const { error: lockErr } = await supabase
    .from('export_job')
    .update({ status: 'running', started_at: new Date().toISOString() })
    .eq('id', job.id)
    .eq('status', 'queued');
  if (lockErr) return;

  try {
    // Example: export applications for a single job_id as CSV
    const payload = job.params_json || {};
    const jobId = payload.job_id;

    const { data: rows, error: qErr } = await supabase
      .from('application')
      .select('id, candidate_id, stage, fit_score, created_at')
      .eq('job_id', jobId);
    if (qErr) throw qErr;

    const csv = [
      ['id','candidate_id','stage','fit_score','created_at'].join(','),
      ...(rows || []).map(r =>
        [r.id, r.candidate_id, r.stage, r.fit_score ?? '', r.created_at].join(','))
    ].join('\n');

    const path = `exports/job_${jobId}_${Date.now()}.csv`;

    // Upload via Buffer (works reliably on Node)
    const { error: upErr } = await supabase.storage
      .from('exports')
      .upload(path, Buffer.from(csv), { upsert: true, contentType: 'text/csv' });
    if (upErr) throw upErr;

    // Signed URL for download (24h)
    const { data: signed, error: urlErr } = await supabase.storage
      .from('exports')
      .createSignedUrl(path, 60 * 60 * 24);
    if (urlErr) throw urlErr;

    await supabase
      .from('export_job')
      .update({
        status: 'completed',
        files_json: { url: signed.signedUrl, path },
        finished_at: new Date().toISOString()
      })
      .eq('id', job.id);
  } catch (e) {
    await supabase
      .from('export_job')
      .update({ status: 'failed', error_json: { message: e.message } })
      .eq('id', job.id);
    console.error('Export error:', e);
  }
}

async function loop() {
  const supabase = getAdminClient();
  while (true) {
    try { await runOne(supabase); }
    catch (e) { console.error('Loop error:', e); }
    await sleep(LOOP_MS);
  }
}

process.on('unhandledRejection', console.error);
process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
loop();
