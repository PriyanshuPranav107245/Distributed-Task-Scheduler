const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const WORKER_ID = process.env.WORKER_ID || `worker-${uuidv4().slice(0, 8)}`;
const API_URL = process.env.API_URL || 'http://localhost:3000';
const POLL_INTERVAL_MS = 2000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processTask(task) {
  try {
    const processingMs = task.estimatedMs || 1000;
    console.log(`[${WORKER_ID}] Processing task ${task.id} type=${task.type} retries=${task.retries}`);
    await sleep(processingMs);

    const failureRoll = Math.random();
    const failRate = typeof task.failRate === 'number' ? task.failRate : 0.12;
    if (failureRoll < failRate) {
      throw new Error('Simulated transient failure');
    }

    await axios.post(`${API_URL}/worker/result`, {
      workerId: WORKER_ID,
      taskId: task.id,
      status: 'success',
    });
    console.log(`[${WORKER_ID}] Completed task ${task.id}`);
  } catch (err) {
    console.warn(`[${WORKER_ID}] Task failed: ${err.message || err}.`);
    const retries = (task.retries || 0) + 1;
    if (retries <= (task.maxRetries || 3)) {
      task.retries = retries;
      await axios.post(`${API_URL}/task/requeue`, { task }).catch((requeueErr) => {
        console.error(`[${WORKER_ID}] Requeue failed: ${requeueErr.message || requeueErr}`);
      });
      console.log(`[${WORKER_ID}] Requeued task ${task.id} retry ${retries}`);
    } else {
      await axios.post(`${API_URL}/worker/result`, {
        workerId: WORKER_ID,
        taskId: task.id,
        status: 'failed',
        error: err.message,
      }).catch(() => {});
      console.error(`[${WORKER_ID}] Task ${task.id} failed permanently after ${retries - 1} retries.`);
    }
  }
}

async function pollLoop() {
  try {
    const resp = await axios.get(`${API_URL}/worker/poll`, {
      params: { workerId: WORKER_ID },
      timeout: 10000,
      validateStatus: () => true,
    });

    if (resp.status === 204) {
      console.log(`[${WORKER_ID}] No task available. Polling again in ${POLL_INTERVAL_MS}ms`);
    } else if (resp.status === 200 && resp.data) {
      await processTask(resp.data);
    } else {
      console.warn(`[${WORKER_ID}] Poll error status=${resp.status}`, resp.data || '');
    }
  } catch (err) {
    console.error(`[${WORKER_ID}] Poll request failed: ${err.message || err}`);
  } finally {
    setTimeout(pollLoop, POLL_INTERVAL_MS);
  }
}

console.log(`[${WORKER_ID}] Starting worker against ${API_URL}`);
pollLoop();
