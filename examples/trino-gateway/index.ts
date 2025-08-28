import { createServer, Socket } from 'node:net';
import { PostgresConnection, FrontendMessageCode } from 'pg-gateway';
import { createClient } from 'redis';
import { randomUUID } from 'node:crypto';
import { Writer } from 'pg-protocol/dist/buffer-writer.js';

// --- Configuration ---
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const GATEWAY_PORT = 5432;
const QUERY_QUEUE = 'trino-query-queue';
const RESULT_QUEUE_PREFIX = 'trino-result:';

// --- Types ---
type Credentials = {
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

// --- Main Application ---

// 1. Initialize Redis Client
const redisClient = createClient({ url: REDIS_URL });
redisClient.on('error', (err) => console.error('Redis Client Error', err));

// 2. Create the Gateway Server
const server = createServer(async (socket) => {
  console.log('Client connected');

  const credentialsStore: Credentials = {};

  const connection = new PostgresConnection(socket, {
    serverVersion: '15.0 (Trino Gateway)',
    authMode: 'cleartextPassword',

    // This hook validates the user's credentials.
    // For our PoC, we'll accept any credentials but store them for later.
    validateCredentials: (credentials) => {
      if (credentials.authMode === 'cleartextPassword') {
        credentialsStore.user = credentials.user;
        credentialsStore.password = credentials.password;
        console.log(`Authenticated user: ${credentials.user}`);
        return true;
      }
      return false;
    },

    // This hook intercepts raw messages from the client.
    // We use it to catch 'Query' messages and handle them ourselves.
    onMessage: async (data, state) => {
      // We only care about messages after the client has authenticated.
      if (!state.isAuthenticated) {
        return false; // Let pg-gateway handle startup/auth flow
      }

      const messageType = data[0];

      // We are only intercepting simple Query messages.
      if (messageType === FrontendMessageCode.Query) {
        // The query string is a null-terminated C-string that starts at byte 5.
        const query = data.toString('utf8', 5, data.length - 1);
        console.log(`Received query: "${query}"`);

        // Simple queries to handle without going to the worker
        if (query.toLowerCase().startsWith('select version()')) {
            sendSimpleTextResult(connection, 'server_version', '15.0 (Trino Gateway)');
            connection.sendReadyForQuery();
            return true;
        }

        const queryId = randomUUID();
        const job = {
          queryId,
          query,
          user: credentialsStore.user,
          password: credentialsStore.password,
        };

        try {
          // Push the job to the worker queue
          await redisClient.lPush(QUERY_QUEUE, JSON.stringify(job));
          console.log(`Queued job ${queryId}`);

          // Wait for the result from the worker.
          // The `brPop` command blocks until a result is available.
          // The result is a tuple [queueName, message]
          const resultPayload = await redisClient.brPop(
            `${RESULT_QUEUE_PREFIX}${queryId}`,
            0 // 0 means block indefinitely
          );

          if (!resultPayload) {
            throw new Error('Received null payload from Redis');
          }

          const workerResult: WorkerResult = JSON.parse(resultPayload.element);

          // Handle the result from the worker
          if (workerResult.status === 'error') {
            console.error(`Query ${queryId} failed:`, workerResult.error);
            connection.sendError({
              severity: 'ERROR',
              code: workerResult.error?.code || 'XX000',
              message:
                workerResult.error?.message || 'An unknown error occurred',
            });
          } else if (workerResult.data) {
            console.log(`Query ${queryId} succeeded`);
            const { columns, rows, command, rowCount } = workerResult.data;

            // If there are columns, it's a SELECT-like query.
            // Send RowDescription followed by DataRows.
            if (columns && columns.length > 0) {
              sendRowDescription(connection, columns);
              rows.forEach((row) => sendDataRow(connection, row));
            }

            // Send CommandComplete to signal success.
            // e.g., "SELECT 2" or "INSERT 0 1"
            const commandTag = `${command} ${rowCount}`;
            sendCommandComplete(connection, commandTag);
          }
        } catch (err) {
          console.error('Gateway error:', err);
          connection.sendError({
            severity: 'FATAL',
            code: 'XX000',
            message:
              err instanceof Error ? err.message : 'An unexpected error occurred in the gateway.',
          });
        }

        // Tell the client we are ready for the next query.
        connection.sendReadyForQuery();

        // Return true to signify that we've handled this message.
        return true;
      }

      // For all other message types, let pg-gateway handle them.
      return false;
    },
  });

  socket.on('end', () => {
    console.log('Client disconnected');
  });

  socket.on('error', (err) => {
    console.error('Socket error:', err);
  });
});

// --- Protocol Helper Functions ---

function sendRowDescription(
  connection: PostgresConnection,
  columns: { name: string; typeOID: number }[]
) {
  const writer = new Writer();
  writer.addInt16(columns.length); // Number of fields

  columns.forEach((col) => {
    writer.addCString(col.name); // Field name
    writer.addInt32(0); // Table OID (0 if not applicable)
    writer.addInt16(0); // Column index (0 if not applicable)
    writer.addInt32(col.typeOID); // Type OID
    writer.addInt16(-1); // Type size (-1 for variable size like text)
    writer.addInt32(-1); // Type modifier (-1 for default)
    writer.addInt16(0); // Format code (0 for text, 1 for binary)
  });

  // This is a hack. The message code for RowDescription is 'T', but the enum in pg-gateway is wrong.
  // 'T' is 84.
  const message = writer.flush(84); // 'T' for RowDescription
  connection.sendData(message);
}

function sendDataRow(connection: PostgresConnection, row: string[]) {
  const writer = new Writer();
  writer.addInt16(row.length); // Number of columns in this row

  row.forEach((col) => {
    if (col === null) {
      writer.addInt32(-1); // -1 for NULL
    } else {
      const colBuffer = Buffer.from(col, 'utf8');
      writer.addInt32(colBuffer.length); // Length of the column value
      writer.add(colBuffer); // The column value itself
    }
  });

  // This is a hack. The message code for DataRow is 'D', but the enum in pg-gateway is wrong.
  // 'D' is 68.
  const message = writer.flush(68); // 'D' for DataRow
  connection.sendData(message);
}

function sendCommandComplete(connection: PostgresConnection, tag: string) {
  const writer = new Writer();
  writer.addCString(tag);
  // This is a hack. The message code for CommandComplete is 'C', but the enum in pg-gateway is wrong.
  // 'C' is 67.
  const message = writer.flush(67); // 'C' for CommandComplete
  connection.sendData(message);
}

// A helper for sending a single result, like from SELECT VERSION()
function sendSimpleTextResult(connection: PostgresConnection, colName: string, value: string) {
    sendRowDescription(connection, [{ name: colName, typeOID: 25 }]); // 25 is OID for text
    sendDataRow(connection, [value]);
    sendCommandComplete(connection, 'SELECT 1');
}

// --- Start Server ---

(async () => {
  try {
    await redisClient.connect();
    server.listen(GATEWAY_PORT, () => {
      console.log(`Postgres Gateway listening on port ${GATEWAY_PORT}`);
      console.log('Waiting for connections...');
    });
  } catch (err) {
      console.error("Failed to connect to Redis. Please ensure Redis is running.", err);
      process.exit(1);
  }
})();
