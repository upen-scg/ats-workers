import fetch from 'node-fetch';
import { getAdminClient } from '../shared/supabaseClient.js';
import { sleep, loopMs } from '../shared/utils.js';

const LOOP_MS = loopMs('PARSER_LOOP_MS', 15000);

async function signedResumeUrl(supabase, path) {
  const { data, error } = await supabase.storage.from('resumes').createSignedUrl(path, 600);
  if (error) throw error;
  return data.signedUrl;
}

// super-basic scoring; swap with BM25 later (Phase 5)
function simpleScore({ resumeText = '', jdText = '', skills = [], required = [] }) {
  const txt = (resumeText || '').toLowerCase();
  const jd = (jdText || '').toLowerCase();
  const hitSkills = skills.filter(s => txt.includes((s || '').toLowerCase()));
  const needHits = required.filter(s => txt.includes((s || '').toLowerCase()));
  const overlap = jd.split(/\W+/).filter(t => t && txt.includes(t)).length;
  const sim = Math.min(100, Math.round((overlap / 200) * 100));
  const kw = Math.min(100, Math.round((hitSkills.length / Math.max(1, skills.length)) * 100));
  const rule = Math.min(100, Math.round((needHits.length / Math.max(1, required.length)) * 100));
  return { finalScore: Math.round(0.5 * kw + 0.3 * sim + 0.2 * rule), details: { hitSkills, needHits, sim, kw, rule } };
}

async function parseWithAffinda(url) {
  const apiKey = process.env.AFFINDA_API_KEY;
  if (!apiKey) throw new Error('Missing AFFINDA_API_KEY');

  const res = await fetch('https://api.affinda.com/v3/resumes', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ url })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Affinda error ${res.status}: ${text}`);
  }
  return res.json();
}

async function loop() {
  const supabase = getAdminClient();

  while (true) {
    try {
      const { data: queued, error } = await supabase
        .from('resume_document')
        .select('*')
        .eq('parse_status', 'queued')
        .limit(5);
      if (error) throw error;

      for (const doc of queued || []) {
        try {
          // best-effort lock
          const { error: lockErr } = await supabase
            .from('resume_document')
            .update({ parse_status: 'processing', started_at: new Date().toISOString() })
            .eq('id', doc.id)
            .eq('parse_status', 'queued');
          if (lockErr) continue;

          const url = await signedResumeUrl(supabase, doc.file_uri);

          const parsed = await parseWithAffinda(url);

          await supabase
            .from('resume_document')
            .update({ parse_status: 'parsed', parsed_json: parsed, parsed_at: new Date().toISOString() })
            .eq('id', doc.id);

          if (doc.candidate_id) {
            // score each application for this candidate
            const { data: apps } = await supabase
              .from('application')
              .select('id, job:job_id(id, jd_text, skills_json, required_skills_json)')
              .eq('candidate_id', doc.candidate_id);

            const resumeText =
              parsed?.data?.professionalSummary ||
              (parsed?.data?.skills || []).map(s => s.name).join(' ') ||
              JSON.stringify(parsed);

            for (const app of apps || []) {
              const { finalScore, details } = simpleScore({
                resumeText,
                jdText: app?.job?.jd_text || '',
                skills: app?.job?.skills_json || [],
                required: app?.job?.required_skills_json || []
              });

              await supabase
                .from('application')
                .update({ fit_score: finalScore, fit_explain_json: details, updated_at: new Date().toISOString() })
                .eq('id', app.id);

              await supabase
                .from('audit_log')
                .insert({
                  actor: 'worker/parser',
                  action: 'score.update',
                  entity_type: 'application',
                  entity_id: app.id,
                  meta: details
                });
            }
          }
        } catch (e) {
          await supabase
            .from('resume_document')
            .update({ parse_status: 'failed', error_json: { message: e.message } })
            .eq('id', doc.id);
          console.error('Parser error:', e);
        }
      }
    } catch (outer) {
      console.error('Loop error:', outer);
    }
    await sleep(LOOP_MS);
  }
}

process.on('unhandledRejection', console.error);
process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));
loop();
