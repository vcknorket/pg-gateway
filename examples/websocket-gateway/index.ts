import { createServer, Server } from 'node:net';
import { PostgresConnection, FrontendMessageCode } from 'pg-gateway';
import WebSocket from 'ws';
import { Writer } from 'pg-protocol/dist/buffer-writer.js';

// ===================================================================================
//
//  This file contains the logic for the PostgreSQL gateway, refactored to support
//  a streaming WebSocket protocol.
//
// ===================================================================================


// --- Configuration (from environment variables with defaults) ---
const PG_PORT = parseInt(process.env.PG_PORT || '5432', 10);
const WORKER_URL = process.env.WORKER_URL || 'ws://localhost:8080';

// --- Types for new WebSocket Protocol ---
export type Job = {
    api_key: string;
    access_token: string;
    query: string;
};

export type WorkerSchema = {
    columns: { name: string; typeOID: number }[];
};

export type WorkerData = (string | null)[][];

export type WorkerComplete = {
    commandTag: string;
    total_rows: number;
};

export type WorkerError = {
    message: string;
    code: string;
};

export type WorkerMessage = {
    query_id: string;
    type: 'schema' | 'data' | 'complete' | 'error';
    payload: WorkerSchema | WorkerData | WorkerComplete | WorkerError;
};


// --- Constants ---
const MSG_CODE = {
    ROW_DESCRIPTION: 84, // 'T'
    DATA_ROW: 68,        // 'D'
    COMMAND_COMPLETE: 67, // 'C'
};
const PG_ERROR_CODES = {
    CONNECTION_FAILURE: '08006',
};

// --- Main Gateway Logic ---
export function createGateway(): Server {
    const server = createServer((socket) => {
        const credentialsStore: { user?: string; password?: string } = {};

        // The state for the current, single in-flight query on this connection.
        // A more advanced implementation would use a Map to handle multiple in-flight queries.
        let inFlightQuery: {
            isSchemaSent: boolean;
            resolve: () => void;
            reject: (err: Error) => void;
        } | null = null;

        const ws = new WebSocket(WORKER_URL);

        const connection = new PostgresConnection(socket, {
            serverVersion: '15.0 (Streaming Gateway)',
            authMode: 'cleartextPassword',
            validateCredentials: (credentials) => {
                credentialsStore.user = credentials.user;
                credentialsStore.password = credentials.password;
                return true;
            },
            onMessage: async (data, state) => {
                if (!state.isAuthenticated) return false;

                if (data[0] === FrontendMessageCode.Query) {
                    const query = data.toString('utf8', 5, data.length - 1);
                    console.log(`[Gateway] Received query: "${query}"`);

                    if (ws.readyState !== WebSocket.OPEN) {
                        connection.sendError({ severity: 'ERROR', code: PG_ERROR_CODES.CONNECTION_FAILURE, message: 'Backend worker is not connected.' });
                        connection.sendReadyForQuery();
                        return true;
                    }

                    const job: Partial<Job> = {
                        api_key: credentialsStore.user,
                        access_token: credentialsStore.password,
                        query: query,
                    };

                    // Wait for the current query to complete before sending the next one.
                    if (inFlightQuery) {
                        connection.sendError({ severity: 'ERROR', code: '55P03', message: 'Gateway is busy with another query.' });
                        connection.sendReadyForQuery();
                        return true;
                    }

                    // Set up the promise that will resolve when the 'complete' or 'error' message arrives.
                    const queryDonePromise = new Promise<void>((resolve, reject) => {
                        inFlightQuery = { isSchemaSent: false, resolve, reject };
                    });

                    ws.send(JSON.stringify(job));

                    await queryDonePromise;

                    // The promise is resolved/rejected by the message handler, which also sends the final PG message.
                    inFlightQuery = null;
                    return true;
                }
                return false;
            },
        });

        ws.on('message', (data) => {
            if (!inFlightQuery) return; // Ignore messages if we're not expecting any.

            const message: WorkerMessage = JSON.parse(data.toString());

            switch (message.type) {
                case 'schema':
                    sendRowDescription(connection, (message.payload as WorkerSchema).columns);
                    inFlightQuery.isSchemaSent = true;
                    break;

                case 'data':
                    if (!inFlightQuery.isSchemaSent) {
                        // Protocol violation
                        const err = new Error('Worker sent data before schema.');
                        connection.sendError({ severity: 'ERROR', code: 'XX000', message: err.message });
                        inFlightQuery.reject(err);
                    } else {
                        (message.payload as WorkerData).forEach(row => sendDataRow(connection, row));
                    }
                    break;

                case 'complete':
                    sendCommandComplete(connection, (message.payload as WorkerComplete).commandTag);
                    connection.sendReadyForQuery();
                    inFlightQuery.resolve();
                    break;

                case 'error':
                    const errorPayload = message.payload as WorkerError;
                    connection.sendError({ severity: 'ERROR', code: errorPayload.code, message: errorPayload.message });
                    connection.sendReadyForQuery();
                    inFlightQuery.reject(new Error(errorPayload.message));
                    break;
            }
        });

        // Cleanup and error handling
        const cleanup = () => {
            if (inFlightQuery) {
                inFlightQuery.reject(new Error('Connection closed unexpectedly.'));
            }
            if (ws.readyState === WebSocket.OPEN) ws.close();
            if (!socket.destroyed) socket.end();
        };
        ws.on('close', cleanup);
        ws.on('error', cleanup);
        socket.on('close', cleanup);
        socket.on('error', cleanup);
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
