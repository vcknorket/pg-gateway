import { createClient } from 'redis';

// --- Configuration ---
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const QUERY_QUEUE = 'trino-query-queue';
const RESULT_QUEUE_PREFIX = 'trino-result:';
const POLLING_TIMEOUT = 0; // 0 means block indefinitely

// --- Types ---
type Job = {
  queryId: string;
  query: string;
  user?: string;
  password?: string;
};

type WorkerResult = {
  status: 'success' | 'error';
  data?: {
    columns: { name: string; typeOID: number }[];
    rows: string[][];
    command: string;
    rowCount: number;
  };
  error?: {
    message: string;
    code: string;
  };
};

// --- Main Worker Logic ---
async function main() {
  const redisClient = createClient({ url: REDIS_URL });
  await redisClient.connect();

  console.log('Worker started. Waiting for queries from the queue...');

  while (true) {
    try {
      // Block until a job is available in the queue
      const jobPayload = await redisClient.blPop(QUERY_QUEUE, POLLING_TIMEOUT);

      if (!jobPayload) continue;

      const job: Job = JSON.parse(jobPayload.element);
      console.log(`Processing job ${job.queryId}: "${job.query}"`);

      // Simulate query execution
      const result = await executeQuery(job.query);

      // Push the result back to the gateway
      const resultQueue = `${RESULT_QUEUE_PREFIX}${job.queryId}`;
      await redisClient.lPush(resultQueue, JSON.stringify(result));

      console.log(`Finished job ${job.queryId}`);
    } catch (err) {
      console.error('Worker loop error:', err);
      // If there's an error, wait a bit before retrying to avoid spamming.
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

/**
 * Simulates executing a query against Trino.
 * In a real implementation, this would connect to Trino, run the query,
 * and format the results.
 */
async function executeQuery(query: string): Promise<WorkerResult> {
  // Simulate network delay
  await new Promise((resolve) => setTimeout(resolve, 200 + Math.random() * 500));

  const lowerCaseQuery = query.toLowerCase().trim();

  // Case 1: A simple SELECT with a literal
  if (lowerCaseQuery === 'select 1' || lowerCaseQuery === 'select 1;') {
    return {
      status: 'success',
      data: {
        columns: [{ name: '?column?', typeOID: 23 }], // 23 = int4
        rows: [['1']],
        command: 'SELECT',
        rowCount: 1,
      },
    };
  }

  // Case 2: A simulated table query
  if (lowerCaseQuery.includes('from users')) {
    return {
      status: 'success',
      data: {
        columns: [
          { name: 'id', typeOID: 23 }, // int4
          { name: 'name', typeOID: 25 }, // text
          { name: 'email', typeOID: 25 }, // text
        ],
        rows: [
          ['1', 'Alice', 'alice@example.com'],
          ['2', 'Bob', 'bob@example.com'],
          ['3', 'Charlie', 'charlie@example.com'],
        ],
        command: 'SELECT',
        rowCount: 3,
      },
    };
  }

  // Case 3: A query that we want to simulate an error for
  if (lowerCaseQuery.includes('error')) {
    return {
        status: 'error',
        error: {
            message: 'This query was designed to fail!',
            code: 'P0001' // custom error code
        }
    }
  }

  // Default Case: For any other query, return an empty result set
  // This is better than an error for unknown commands.
  const command = query.split(' ')[0].toUpperCase();
  return {
    status: 'success',
    data: {
      columns: [],
      rows: [],
      command: command,
      rowCount: 0,
    },
  };
}

main().catch((err) => {
  console.error('Worker failed to start:', err);
  process.exit(1);
});
