import { DateTime } from 'luxon';
import { readFileSync } from 'node:fs';
import path from 'node:path';

const DEFAULT_QUEUES = [
  { id: '0815efc4-da90-4446-acf7-287205c24d77', name: 'Card' },
  { id: 'ad89dec0-5f05-4d22-81bb-7b2cc67bbfa7', name: 'General' }
];

function readQueuesFromConfigFile() {
  const configPath = path.join(process.cwd(), 'config', 'queues.json');
  const raw = readFileSync(configPath, 'utf8');
  return JSON.parse(raw);
}

function validateQueues(queues) {
  if (!Array.isArray(queues) || queues.length === 0) {
    throw new Error('Queue configuration must be a non-empty array.');
  }

  for (const queue of queues) {
    if (!queue.id || !queue.name) {
      throw new Error('Each queue must have both id and name fields.');
    }
  }

  return queues;
}

const AGGREGATE_METRICS = [
  'oServiceLevel',
  'nOffered',
  'tAbandon',
  'tFlowOut',
  'tAnswered',
  'tHandle',
  'tWait'
];

const OBSERVATION_METRICS = [
  'oWaiting',
  'oInteracting',
  'oAlerting',
  'oMemberUsers',
  'oActiveUsers',
  'oOnQueueUsers',
  'oOffQueueUsers',
  'oLongestWaiting',
  'oLongestInteracting'
];

function getConfig() {
  const region = process.env.GENESYS_REGION || 'mypurecloud.ie';
  const timeZone = process.env.GENESYS_TIMEZONE || 'Europe/Belgrade';
  let queues = DEFAULT_QUEUES;

  if (process.env.QUEUES_JSON) {
    try {
      queues = JSON.parse(process.env.QUEUES_JSON);
    } catch (error) {
      throw new Error(`Invalid QUEUES_JSON: ${error.message}`);
    }
  } else {
    try {
      queues = readQueuesFromConfigFile();
    } catch (error) {
      console.warn(`Could not read config/queues.json, using default queues. Reason: ${error.message}`);
    }
  }

  return { region, timeZone, queues: validateQueues(queues) };
}

function apiBase(region) {
  return `https://api.${region}`;
}

function loginBase(region) {
  return `https://login.${region}`;
}

async function getAccessToken(region) {
  const clientId = process.env.GENESYS_CLIENT_ID;
  const clientSecret = process.env.GENESYS_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error('Missing GENESYS_CLIENT_ID or GENESYS_CLIENT_SECRET environment variables.');
  }

  const basic = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
  const response = await fetch(`${loginBase(region)}/oauth/token`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: 'grant_type=client_credentials'
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Token request failed: ${response.status} ${text}`);
  }

  const json = await response.json();
  return json.access_token;
}

function queueFilter(queues) {
  return {
    type: 'or',
    predicates: queues.map((q) => ({ dimension: 'queueId', value: q.id }))
  };
}

function mediaTypeFilter() {
  return {
    type: 'or',
    predicates: [{ type: 'dimension', dimension: 'mediaType', value: 'voice' }]
  };
}

async function genesysPost(region, token, path, payload) {
  const response = await fetch(`${apiBase(region)}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${path} failed: ${response.status} ${text}`);
  }

  return response.json();
}

function getDayToNowInterval(timeZone) {
  const now = DateTime.now().setZone(timeZone);
  const start = now.startOf('day');
  return `${start.toUTC().toISO({ suppressMilliseconds: false })}/${now.toUTC().toISO({ suppressMilliseconds: false })}`;
}

function metricMap(metrics = []) {
  const map = {};
  for (const item of metrics) map[item.metric] = item.stats || {};
  return map;
}

function safeRatio(stats) {
  if (!stats || typeof stats.ratio !== 'number') return null;
  return stats.ratio;
}

function avg(stats) {
  if (!stats || !stats.count) return null;
  return stats.sum / stats.count;
}

function count(stats) {
  return stats?.count ?? 0;
}

function msToSeconds(ms) {
  if (typeof ms !== 'number') return null;
  return Math.round(ms / 1000);
}

function parseAggregates(queues, aggregateResponse) {
  const result = Object.fromEntries(queues.map((q) => [q.id, {}]));

  for (const groupResult of aggregateResponse.results || []) {
    const queueId = groupResult.group?.queueId;
    if (!queueId || !result[queueId]) continue;
    const data = groupResult.data?.[0];
    const metrics = metricMap(data?.metrics || []);

    result[queueId] = {
      offered: count(metrics.nOffered),
      answered: count(metrics.tAnswered),
      abandoned: count(metrics.tAbandon),
      flowOut: count(metrics.tFlowOut),
      serviceLevel: safeRatio(metrics.oServiceLevel),
      serviceLevelTarget: metrics.oServiceLevel?.target ?? null,
      avgHandleSeconds: msToSeconds(avg(metrics.tHandle)),
      avgWaitSeconds: msToSeconds(avg(metrics.tWait)),
      maxWaitSeconds: msToSeconds(metrics.tWait?.max ?? null),
      totalWaitSeconds: msToSeconds(metrics.tWait?.sum ?? null)
    };
  }

  return result;
}

function parseObservations(queues, observationResponse) {
  const result = Object.fromEntries(queues.map((q) => [q.id, {}]));
  const nowMs = Date.now();

  for (const groupResult of observationResponse.results || []) {
    const queueId = groupResult.group?.queueId;
    if (!queueId || !result[queueId]) continue;

    // Keep this PoC voice-oriented for interaction metrics, but allow user metrics without mediaType.
    const mediaType = groupResult.group?.mediaType;
    const isVoiceOrUserMetricGroup = !mediaType || mediaType === 'voice';
    if (!isVoiceOrUserMetricGroup) continue;

    for (const item of groupResult.data || []) {
      const c = count(item.stats);
      if (item.metric === 'oWaiting') result[queueId].waiting = c;
      if (item.metric === 'oInteracting') result[queueId].interacting = c;
      if (item.metric === 'oAlerting') result[queueId].alerting = c;
      if (item.metric === 'oMemberUsers') result[queueId].memberUsers = c;
      if (item.metric === 'oActiveUsers') result[queueId].activeUsers = c;
      if (item.metric === 'oOnQueueUsers') result[queueId].onQueue = (result[queueId].onQueue || 0) + c;
      if (item.metric === 'oOffQueueUsers') result[queueId].offQueue = (result[queueId].offQueue || 0) + c;
      if (item.metric === 'oLongestWaiting') {
        const startTimestamp = item.stats?.calculatedMetricValue;
        result[queueId].longestWaitingSeconds = typeof startTimestamp === 'number'
          ? Math.max(0, Math.round((nowMs - startTimestamp) / 1000))
          : null;
      }
    }
  }

  return result;
}

function combine(queues, aggregateData, observationData) {
  return queues.map((q) => ({
    id: q.id,
    name: q.name,
    ...aggregateData[q.id],
    ...observationData[q.id]
  }));
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { region, timeZone, queues } = getConfig();
    const token = await getAccessToken(region);
    const interval = getDayToNowInterval(timeZone);

    const aggregatePayload = {
      filter: {
        type: 'and',
        clauses: [queueFilter(queues), mediaTypeFilter()]
      },
      metrics: AGGREGATE_METRICS,
      groupBy: ['queueId'],
      interval
    };

    const observationPayload = {
      metrics: OBSERVATION_METRICS,
      filter: {
        type: 'and',
        clauses: [queueFilter(queues)]
      }
    };

    const [aggregateResponse, observationResponse] = await Promise.all([
      genesysPost(region, token, '/api/v2/analytics/conversations/aggregates/query', aggregatePayload),
      genesysPost(region, token, '/api/v2/analytics/queues/observations/query', observationPayload)
    ]);

    const aggregateData = parseAggregates(queues, aggregateResponse);
    const observationData = parseObservations(queues, observationResponse);

    res.status(200).json({
      generatedAt: new Date().toISOString(),
      timeZone,
      interval,
      queues: combine(queues, aggregateData, observationData),
      debug: req.query.debug === '1' ? { aggregatePayload, observationPayload } : undefined
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
}
