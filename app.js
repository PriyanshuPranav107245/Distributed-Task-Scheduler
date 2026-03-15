const express = require('express');
const Redis = require('ioredis');
const cron = require('node-cron');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = Number(process.env.REDIS_PORT || '6379');
const PORT = Number(process.env.PORT || '3000');
const QUEUE_NAME = 'distributed_task_queue';

let useRedis = true;
const redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
redis.on('error', (err) => {
  useRedis = false;
  console.error('Redis error:', err.message || err);
  console.warn('Falling back to in-memory queue. This is not persistent.');
});

const inMemoryQueue = [];

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const TASK_TYPES = {
  data_processing: { name: 'data_processing', avgMs: 1300, failRate: 0.15 },
  email_batch: { name: 'email_batch', avgMs: 900, failRate: 0.08 },
  report_generation: { name: 'report_generation', avgMs: 1600, failRate: 0.12 },
  cleanup: { name: 'cleanup', avgMs: 700, failRate: 0.05 },
};

const stats = {
  scheduled: 0,
  completed: 0,
  failed: 0,
  totalProcessed: 0,
  activeWorkers: {},
};

function randomTaskType() {
  const keys = Object.keys(TASK_TYPES);
  return TASK_TYPES[keys[Math.floor(Math.random() * keys.length)]];
}

function createTask() {
  const typeData = randomTaskType();
  return {
    id: uuidv4(),
    type: typeData.name,
    payload: {
      dataBatchId: uuidv4(),
      createdAt: new Date().toISOString(),
    },
    retries: 0,
    maxRetries: 3,
    createdAt: new Date().toISOString(),
    estimatedMs: typeData.avgMs,
    failRate: typeData.failRate,
  };
}

app.post('/task/generate', async (req, res) => {
  try {
    const count = Number(req.query.count || 5);
    const tasks = Array.from({ length: Math.max(1, Math.min(count, 20)) }, createTask);
    await Promise.all(tasks.map((task) => pushTask(task)));
    stats.scheduled += tasks.length;
    return res.json({ ok: true, generated: tasks.length });
  } catch (err) {
    return res.status(500).json({ error: 'generate failed', details: err.message || err });
  }
});

async function pushTask(task) {
  if (useRedis) {
    try {
      await redis.lpush(QUEUE_NAME, JSON.stringify(task));
      return;
    } catch (err) {
      useRedis = false;
      console.warn('Redis unavailable; switching to in-memory queue.');
    }
  }
  inMemoryQueue.unshift(task);
}

async function popTask() {
  if (useRedis) {
    try {
      const raw = await redis.rpop(QUEUE_NAME);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (err) {
      useRedis = false;
      console.warn('Redis unavailable; switching to in-memory queue.');
    }
  }
  return inMemoryQueue.length > 0 ? inMemoryQueue.pop() : null;
}

async function queueLength() {
  if (useRedis) {
    try {
      return await redis.llen(QUEUE_NAME);
    } catch {
      useRedis = false;
    }
  }
  return inMemoryQueue.length;
}

cron.schedule('*/5 * * * *', async () => {
  try {
    const nextCount = Math.floor(Math.random() * 5) + 3;
    const tasks = Array.from({ length: nextCount }, (_, i) => createTask());

    await Promise.all(tasks.map((task) => pushTask(task)));
    stats.scheduled += tasks.length;
    const ql = await queueLength();
    console.log(`Generated ${tasks.length} tasks. Queue length: ${ql}`);
  } catch (err) {
    console.error('Cron task generation failed:', err.message || err);
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/worker/poll', async (req, res) => {
  try {
    const workerId = req.query.workerId || 'unknown';
    const task = await popTask();
    stats.activeWorkers[workerId] = Date.now();
    if (!task) {
      return res.status(204).send();
    }
    task.dispatchAt = new Date().toISOString();
    return res.json(task);
  } catch (err) {
    console.error('/worker/poll error:', err.message || err);
    return res.status(500).json({ error: 'Unable to poll task' });
  }
});

app.post('/task/requeue', async (req, res) => {
  try {
    const task = req.body.task;
    if (!task) return res.status(400).json({ error: 'Missing task in body' });
    task.retries = task.retries || 0;
    if (task.retries >= (task.maxRetries ?? 3)) {
      return res.status(400).json({ error: 'Max retries reached' });
    }
    task.retries += 1;
    await pushTask(task);
    return res.json({ ok: true, retries: task.retries });
  } catch (err) {
    console.error('/task/requeue error:', err.message || err);
    return res.status(500).json({ error: 'requeue failed' });
  }
});

app.post('/worker/result', (req, res) => {
  const { workerId, taskId, status } = req.body;
  if (!taskId || !status) return res.status(400).json({ error: 'taskId and status required' });
  if (status === 'success') stats.completed += 1;
  else stats.failed += 1;
  stats.totalProcessed += 1;
  if (workerId) stats.activeWorkers[workerId] = Date.now();
  return res.json({ ok: true, status });
});

app.get('/dashboard', async (req, res) => {
  const endpoint = (req.query.endpoint || 'stats').toString().toLowerCase();
  const queueLen = await queueLength();
  if (endpoint === 'workers') {
    const active = Object.entries(stats.activeWorkers).map(([id, ts]) => ({ workerId: id, lastSeen: new Date(ts).toISOString() }));
    return res.json({ activeWorkers: active, queueLength: queueLen, store: useRedis ? 'redis' : 'memory' });
  }
  return res.json({
    scheduled: stats.scheduled,
    completed: stats.completed,
    failed: stats.failed,
    totalProcessed: stats.totalProcessed,
    queueLength: queueLen,
    activeWorkers: Object.keys(stats.activeWorkers).length,
    store: useRedis ? 'redis' : 'memory',
  });
});

app.get('/health', async (req, res) => {
  try {
    const queueLen = await queueLength();
    return res.json({
      uptimeSeconds: process.uptime(),
      queueLength: queueLen,
      store: useRedis ? 'redis' : 'memory',
      version: '1.0.0',
    });
  } catch (err) {
    return res.status(500).json({ error: 'Health check failed', details: err.message || err });
  }
});

app.listen(PORT, () => {
  console.log(`Scheduler API running on http://localhost:${PORT}`);
  console.log(`Task queue using ${useRedis ? 'Redis' : 'in-memory fallback'}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down...');
  if (useRedis) await redis.quit();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down...');
  if (useRedis) await redis.quit();
  process.exit(0);
});
