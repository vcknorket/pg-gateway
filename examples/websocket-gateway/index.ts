import { createServer, Server } from 'node:net';
import { PostgresConnection, FrontendMessageCode } from 'pg-gateway';
import WebSocket from 'ws';
import { randomUUID } from 'node:crypto';
import { Writer } from 'pg-protocol/dist/buffer-writer.js';
import { EventEmitter } from 'node:events';

// --- Types ---
type Credentials = { user?: string; password?: string; };
export type WorkerResult = {
  queryId: string;
  status: 'success' | 'error';
  payload: {
    columns?: { name: string; typeOID: number }[];
    rows?: (string | null)[][];
    commandTag?: string;
    error?: {
      message: string;
      code: string;
    };
  };
};
export type Job = { queryId:string; query: string; user?: string; password?: string; };

// --- Constants ---
const PG_PORT = 5432;
const WORKER_URL = `ws://localhost:8080`;

// --- Protocol Message Codes (centralized for correctness) ---
const MSG_CODE = {
    ROW_DESCRIPTION: 84, // 'T'
    DATA_ROW: 68,        // 'D'
    COMMAND_COMPLETE: 67, // 'C'
};

/**
 * Creates and configures the PostgreSQL gateway server.
 * @returns A Node.js `net.Server` instance.
 */
export function createGateway(): Server {
    const server = createServer((socket) => {
        console.log('PG client connected');
        const credentialsStore: Credentials = {};

        // For each PG connection, we create a new WebSocket connection to the backend.
        // This provides a simple and effective concurrency model.
        const ws = new WebSocket(WORKER_URL);
        const responseEmitter = new EventEmitter();

        const connection = new PostgresConnection(socket, {
            serverVersion: '15.0 (WebSocket Gateway)',
            authMode: 'cleartextPassword',

            validateCredentials: (credentials) => {
                if (credentials.authMode === 'cleartextPassword') {
                    credentialsStore.user = credentials.user;
                    credentialsStore.password = credentials.password;
                    return true;
                }
                return false;
            },

            onMessage: async (data, state) => {
                if (!state.isAuthenticated) return false;

                if (data[0] === FrontendMessageCode.Query) {
                    const query = data.toString('utf8', 5, data.length - 1);
                    console.log(`[Gateway] Received query: "${query}"`);

                    const queryId = randomUUID();
                    const job: Job = { queryId, query, user: credentialsStore.user };

                    const resultPromise = new Promise<WorkerResult>((resolve) => {
                        responseEmitter.once(queryId, resolve);
                    });

                    ws.send(JSON.stringify(job));

                    const result = await resultPromise;

                    if (result.status === 'error') {
                        connection.sendError({
                            severity: 'ERROR', // Recoverable error
                            code: result.payload.error?.code || 'XX000',
                            message: result.payload.error?.message || 'An unknown error occurred in the worker.',
                        });
                    } else {
                        const { columns, rows, commandTag } = result.payload;
                        if (columns && rows) {
                            sendRowDescription(connection, columns);
                            rows.forEach((row) => sendDataRow(connection, row));
                        }
                        sendCommandComplete(connection, commandTag || '');
                    }

                    connection.sendReadyForQuery();
                    return true;
                }
                return false;
            },
        });

        // --- WebSocket Event Handling ---
        ws.on('open', () => {
            console.log('[Gateway] WebSocket connection to worker established.');
        });

        ws.on('message', (data) => {
            const result: WorkerResult = JSON.parse(data.toString());
            responseEmitter.emit(result.queryId, result);
        });

        ws.on('close', () => {
            console.log('[Gateway] WebSocket connection closed.');
            if (!socket.destroyed) {
                socket.end();
            }
        });

        ws.on('error', (err) => {
            console.error('[Gateway] WebSocket error:', err);
            connection.sendError({
                severity: 'FATAL', // This is a connection-level issue
                code: '08000', // Connection Exception
                message: 'Gateway could not communicate with the backend worker.',
            });
            if (!socket.destroyed) {
                socket.end(); // Close the PG connection
            }
        });

        // --- PG Socket Event Handling ---
        socket.on('close', () => {
            console.log('PG client disconnected.');
            if (ws.readyState === WebSocket.OPEN) {
                ws.close();
            }
        });

        socket.on('error', (err) => {
            console.error('PG socket error:', err);
        });
    });

    return server;
}

// --- Protocol Helper Functions ---
function sendRowDescription(conn: PostgresConnection, cols: { name: string; typeOID: number }[]) {
    const writer = new Writer();
    writer.addInt16(cols.length);
    cols.forEach(c => { writer.addCString(c.name); writer.addInt32(0); writer.addInt16(0); writer.addInt32(c.typeOID); writer.addInt16(-1); writer.addInt32(-1); writer.addInt16(0); });
    conn.sendData(writer.flush(MSG_CODE.ROW_DESCRIPTION));
}

function sendDataRow(conn: PostgresConnection, row: (string | null)[]) {
    const writer = new Writer();
    writer.addInt16(row.length);
    row.forEach(val => {
        if (val === null) {
            writer.addInt32(-1); // -1 for NULL
        } else {
            const buffer = Buffer.from(val, 'utf8');
            writer.addInt32(buffer.length);
            writer.add(buffer);
        }
    });
    conn.sendData(writer.flush(MSG_CODE.DATA_ROW));
}

function sendCommandComplete(conn: PostgresConnection, tag: string) {
    const writer = new Writer();
    writer.addCString(tag);
    conn.sendData(writer.flush(MSG_CODE.COMMAND_COMPLETE));
}


// --- Standalone Execution ---
if (require.main === module) {
    const server = createGateway();
    server.listen(PG_PORT, () => {
        console.log(`PostgreSQL Gateway listening on port ${PG_PORT}`);
    });
}
