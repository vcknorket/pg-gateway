import { createServer, Server } from 'node:net';
import { PostgresConnection, FrontendMessageCode } from 'pg-gateway';
import WebSocket from 'ws';
import { randomUUID } from 'node:crypto';
import { Writer } from 'pg-protocol/dist/buffer-writer.js';
import { EventEmitter } from 'node:events';

// ===================================================================================
//
//  This file contains the logic for the PostgreSQL gateway.
//  It's designed to be robust and includes many of the improvements
//  suggested in the user feedback.
//
// ===================================================================================


// --- Configuration (from environment variables with defaults) ---
const PG_PORT = parseInt(process.env.PG_PORT || '5432', 10);
const WORKER_URL = process.env.WORKER_URL || 'ws://localhost:8080';
const SCHEMA_VERSION = '1.0.0';
const QUERY_TIMEOUT_MS = parseInt(process.env.QUERY_TIMEOUT_MS || '60000', 10);
const PING_INTERVAL_MS = 15000;
const PONG_GRACE_MS = 30000;
const WS_MAX_BUFFERED_AMOUNT = 8 * 1024 * 1024; // 8MB

// --- Types ---
export type Job = {
    schemaVersion: string;
    queryId: string;
    query: string;
    user?: string;
    password?: string;
};

export type WorkerResult = {
    schemaVersion: string;
    queryId:string;
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

// --- Constants ---
const MSG_CODE = {
    ROW_DESCRIPTION: 84, // 'T'
    DATA_ROW: 68,        // 'D'
    COMMAND_COMPLETE: 67, // 'C'
};
const PG_ERROR_CODES = {
    SYNTAX_ERROR: '42601',
    CONNECTION_FAILURE: '08006',
    QUERY_CANCELED: '57014',
};

/**
 * Creates and configures the PostgreSQL gateway server.
 * @returns A Node.js `net.Server` instance.
 */
export function createGateway(): Server {
    const server = createServer((socket) => {
        console.log('PG client connected');
        const credentialsStore: { user?: string, password?: string } = {};

        const ws = new WebSocket(WORKER_URL);
        const responseEmitter = new EventEmitter();

        // --- Robustness: WebSocket Heartbeat ---
        let lastPong = Date.now();
        const pingInterval = setInterval(() => {
            if (ws.readyState !== WebSocket.OPEN) return;
            if (Date.now() - lastPong > PONG_GRACE_MS) {
                console.error('[Gateway] WebSocket connection timed out (no pong). Terminating.');
                return ws.terminate();
            }
            ws.ping();
        }, PING_INTERVAL_MS);
        ws.on('pong', () => { lastPong = Date.now(); });


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

                    console.log(`[Gateway] Checking WS state before query: ${ws.readyState} (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)`);
                    if (ws.readyState !== WebSocket.OPEN) {
                        connection.sendError({ severity: 'ERROR', code: PG_ERROR_CODES.CONNECTION_FAILURE, message: 'Gateway is not connected to the backend worker. Please try again.' });
                        connection.sendReadyForQuery();
                        return true;
                    }

                    // --- Fast-path for version query ---
                    const lq = query.trim().toLowerCase();
                    if (lq.startsWith('select version()')) {
                        sendRowDescription(connection, [{ name: 'version', typeOID: 25 }]);
                        sendDataRow(connection, [`${connection.options.serverVersion} (via pg-gateway)`]);
                        sendCommandComplete(connection, 'SELECT 1');
                        connection.sendReadyForQuery();
                        return true;
                    }

                    // Multi-statement query detection is complex due to strings and comments.
                    // A simple check can have false positives. For this MVP, we are omitting
                    // the check, but a production system would need a more robust parser.

                    const queryId = randomUUID();
                    const job: Job = { schemaVersion: SCHEMA_VERSION, queryId, query, user: credentialsStore.user, password: credentialsStore.password };

                    const resultPromise = new Promise<WorkerResult>((resolve) => {
                        responseEmitter.once(queryId, resolve);
                    });

                    try {
                        // --- Graceful Connect: Wait for WS to be open ---
                        await waitWsOpen(ws);

                        const resultPromise = new Promise<WorkerResult>((resolve) => {
                            responseEmitter.once(queryId, resolve);
                        });

                        // --- Robustness: Query Timeout ---
                        const timeoutPromise = new Promise<never>((_, reject) =>
                            setTimeout(() => reject(new Error('Timed out waiting for worker response.')), QUERY_TIMEOUT_MS)
                        );

                        wsSendSafe(ws, job);
                        const result = await Promise.race([resultPromise, timeoutPromise]);

                        if (result.status === 'error') {
                            connection.sendError({ severity: 'ERROR', code: result.payload.error?.code || 'XX000', message: result.payload.error?.message || 'An unknown error occurred.' });
                        } else {
                            const { columns, rows, commandTag } = result.payload;
                            if (columns) {
                                sendRowDescription(connection, columns);
                                if (rows) rows.forEach((row) => sendDataRow(connection, row));
                            }
                            sendCommandComplete(connection, commandTag || 'SELECT 0');
                        }
                    } catch (err) {
                        // This single catch block now handles errors from waitWsOpen, wsSendSafe, and the query timeout.
                        connection.sendError({ severity: 'ERROR', code: PG_ERROR_CODES.QUERY_CANCELED, message: (err as Error).message });
                    }

                    connection.sendReadyForQuery();
                    return true;
                }
                return false;
            },
        });

        // --- Robustness: Response Validation ---
        ws.on('message', (data) => {
            let result: WorkerResult;
            try {
                result = JSON.parse(data.toString());
                // Basic shape validation
                if (!result?.queryId || !result?.status || !result?.payload || result.schemaVersion !== SCHEMA_VERSION) {
                    console.error('[Gateway] Received invalid message from worker:', result);
                    return;
                }
                responseEmitter.emit(result.queryId, result);
            } catch (e) {
                console.error('[Gateway] Failed to parse message from worker:', e);
            }
        });

        ws.on('close', () => {
            console.log('[Gateway] WebSocket connection closed.');
            clearInterval(pingInterval); // Graceful teardown
            // Do NOT end the PG socket. The session remains alive.
        });

        ws.on('error', (err) => {
            console.error('[Gateway] WebSocket error:', err);
            clearInterval(pingInterval); // Graceful teardown
            // Do not send a FATAL error or end the PG socket.
            // Future queries will fail gracefully due to the readyState check.
        });

        socket.on('close', () => {
             console.log('PG client disconnected.');
             clearInterval(pingInterval); // Graceful teardown
             if (ws.readyState === WebSocket.OPEN) ws.close();
        });
        socket.on('error', (err) => console.error('PG socket error:', err));
    });

    return server;
}

// --- Robustness: Backpressure Handling & Graceful Connect ---
async function waitWsOpen(ws: WebSocket, ms = 2000): Promise<void> {
    if (ws.readyState === WebSocket.OPEN) return;
    if (ws.readyState !== WebSocket.CONNECTING) throw new Error('Worker socket is not connecting.');

    return new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Timed out waiting for worker to connect.')), ms);
        // Both 'open' and 'error' events should resolve the promise
        const onOpen = () => {
            clearTimeout(timeout);
            ws.removeListener('error', onError);
            resolve();
        };
        const onError = (err: Error) => {
            clearTimeout(timeout);
            ws.removeListener('open', onOpen);
            reject(err);
        };
        ws.once('open', onOpen);
        ws.once('error', onError);
    });
}

function wsSendSafe(ws: WebSocket, payload: any) {
    if (ws.readyState !== WebSocket.OPEN) {
        // This should not be hit if waitWsOpen is used, but serves as a final guard.
        throw new Error('Worker not connected');
    }
    if (ws.bufferedAmount > WS_MAX_BUFFERED_AMOUNT) {
        console.error(`[Gateway] WebSocket backpressure limit exceeded (${ws.bufferedAmount} bytes).`);
        throw new Error('Worker connection is overloaded.');
    }
    ws.send(JSON.stringify(payload));
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
        if (val === null) writer.addInt32(-1);
        else {
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
