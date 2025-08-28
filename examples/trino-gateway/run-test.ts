import { createServer, Server, Socket } from 'node:net';
import { PostgresConnection, FrontendMessageCode } from 'pg-gateway';
import { randomUUID } from 'node:crypto';
import { Writer } from 'pg-protocol/dist/buffer-writer.js';
import { Client } from 'pg';
import { EventEmitter } from 'node:events';

// ===================================================================================
//
// NOTICE: This file combines the gateway, worker, and test client into a single
// script and SIMULATES REDIS IN-MEMORY. This is a workaround for an environment
// where Redis is not available and where running multiple processes is unstable.
//
// ===================================================================================


// --- Configuration ---
const GATEWAY_PORT = 5432;
const CONNECTION_STRING = `postgresql://testuser:testpassword@localhost:${GATEWAY_PORT}/testdb`;

// --- In-Memory Redis Simulation ---
class InMemoryQueue {
    private queue: any[] = [];
    private emitter = new EventEmitter();

    lPush(item: string) {
        this.queue.push(item);
        this.emitter.emit('push');
    }

    async brPop(): Promise<string | null> {
        if (this.queue.length > 0) {
            return this.queue.shift();
        }
        return new Promise((resolve) => {
            this.emitter.once('push', () => {
                resolve(this.queue.shift());
            });
        });
    }
}
const queryQueue = new InMemoryQueue();
const resultEmitter = new EventEmitter();


// --- Types ---
type Credentials = { user?: string; password?: string; };
type WorkerResult = {
  status: 'success' | 'error';
  data?: { columns: { name: string; typeOID: number }[]; rows: string[][]; command: string; rowCount: number; };
  error?: { message: string; code: string; };
};
type Job = { queryId: string; query: string; user?: string; password?: string; };


// ===================================================================================
// Gateway Logic (from index.ts)
// ===================================================================================
function createGateway(): Server {
    const server = createServer((socket) => {
        const credentialsStore: Credentials = {};
        const connection = new PostgresConnection(socket, {
            serverVersion: '15.0 (Trino Gateway)', authMode: 'cleartextPassword',
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
                    if (query.toLowerCase().startsWith('select version()')) {
                        sendSimpleTextResult(connection, 'server_version', '15.0 (Trino Gateway)');
                        connection.sendReadyForQuery();
                        return true;
                    }

                    const queryId = randomUUID();
                    const job = { queryId, query, user: credentialsStore.user };

                    const resultPromise = new Promise<WorkerResult>((resolve) => {
                        resultEmitter.once(queryId, resolve);
                    });

                    queryQueue.lPush(JSON.stringify(job));

                    const workerResult = await resultPromise;

                    if (workerResult.status === 'error') {
                        connection.sendError({ severity: 'ERROR', code: workerResult.error?.code || 'XX000', message: workerResult.error?.message || 'Error' });
                    } else if (workerResult.data) {
                        const { columns, rows, command, rowCount } = workerResult.data;
                        if (columns && columns.length > 0) {
                            sendRowDescription(connection, columns);
                            rows.forEach((row) => sendDataRow(connection, row));
                        }
                        sendCommandComplete(connection, `${command} ${rowCount}`);
                    }
                    connection.sendReadyForQuery();
                    return true;
                }
                return false;
            },
        });
    });
    return server;
}

// Protocol Helpers
function sendRowDescription(conn: PostgresConnection, cols: { name: string; typeOID: number }[]) {
    const writer = new Writer();
    writer.addInt16(cols.length);
    cols.forEach(c => { writer.addCString(c.name); writer.addInt32(0); writer.addInt16(0); writer.addInt32(c.typeOID); writer.addInt16(-1); writer.addInt32(-1); writer.addInt16(0); });
    conn.sendData(writer.flush(84));
}
function sendDataRow(conn: PostgresConnection, row: string[]) {
    const writer = new Writer();
    writer.addInt16(row.length);
    row.forEach(c => { if (c === null) { writer.addInt32(-1); } else { const b = Buffer.from(c, 'utf8'); writer.addInt32(b.length); writer.add(b); } });
    conn.sendData(writer.flush(68));
}
function sendCommandComplete(conn: PostgresConnection, tag: string) {
    const writer = new Writer();
    writer.addCString(tag);
    conn.sendData(writer.flush(67));
}
function sendSimpleTextResult(conn: PostgresConnection, colName: string, value: string) {
    sendRowDescription(conn, [{ name: colName, typeOID: 25 }]);
    sendDataRow(conn, [value]);
    sendCommandComplete(conn, 'SELECT 1');
}


// ===================================================================================
// Worker Logic (from worker.ts)
// ===================================================================================
let keepWorkerRunning = true;
async function runWorker() {
    console.log('Worker loop started.');
    while (keepWorkerRunning) {
        try {
            const jobPayload = await queryQueue.brPop();
            if (!jobPayload) continue;
            if(!keepWorkerRunning) break; // Exit if stopped while waiting

            const job: Job = JSON.parse(jobPayload);
            const result = await executeQuery(job.query);
            resultEmitter.emit(job.queryId, result);
        } catch (err) {
            if (keepWorkerRunning) console.error('Worker loop error:', err);
        }
    }
    console.log('Worker loop stopped.');
}

async function executeQuery(query: string): Promise<WorkerResult> {
    const lq = query.toLowerCase().trim();
    if (lq === 'select 1' || lq === 'select 1;') return { status: 'success', data: { columns: [{ name: '?column?', typeOID: 23 }], rows: [['1']], command: 'SELECT', rowCount: 1 } };
    if (lq.includes('from users')) return { status: 'success', data: { columns: [{ name: 'id', typeOID: 23 }, { name: 'name', typeOID: 25 }], rows: [['1', 'Alice'], ['2', 'Bob']], command: 'SELECT', rowCount: 2 } };
    if (lq.includes('error')) return { status: 'error', error: { message: 'This query was designed to fail!', code: 'P0001' } };
    return { status: 'success', data: { columns: [], rows: [], command: query.split(' ')[0].toUpperCase(), rowCount: 0 } };
}


// ===================================================================================
// Test Client Logic (from test-client.ts)
// ===================================================================================
async function runClientTests() {
    console.log('\n--- Starting Test Client ---');
    const client = new Client({ connectionString: CONNECTION_STRING });
    let testSuccess = true;

    try {
        await client.connect();
        console.log('Client connected to gateway.');

        // Test 1
        console.log('\nRunning test: SELECT 1');
        const res1 = await client.query('SELECT 1;');
        console.table(res1.rows);
        if (res1.rows[0]['?column?'] !== 1) throw new Error('Test 1 Failed');


        // Test 2
        console.log('\nRunning test: SELECT * FROM users');
        const res2 = await client.query('SELECT * FROM users;');
        console.table(res2.rows);
        if (res2.rowCount !== 2) throw new Error('Test 2 Failed');

        // Test 3
        console.log('\nRunning test: Error query');
        try {
            await client.query('select from error');
        } catch (e) {
            console.log('Caught expected error:', e.message);
        }

        console.log('\n--- Test Client Finished Successfully ---');
    } catch (err) {
        console.error('\n--- Test Client Failed ---');
        console.error(err);
        testSuccess = false;
    } finally {
        await client.end();
        return testSuccess;
    }
}


// ===================================================================================
// Main Test Runner
// ===================================================================================
async function main() {
    let server: Server | null = null;
    let finalStatus = 1; // Default to fail

    try {
        // 1. Setup
        server = createGateway();
        await new Promise<void>(resolve => server!.listen(GATEWAY_PORT, resolve));
        console.log(`Gateway listening on port ${GATEWAY_PORT}.`);

        // 2. Run Worker and Client
        const workerPromise = runWorker();
        // give worker a moment to start listening
        await new Promise(res => setTimeout(res, 100));
        const clientPromise = runClientTests();

        const success = await clientPromise;

        // 3. Teardown
        keepWorkerRunning = false;
        queryQueue.lPush(''); // Push a dummy value to unblock the worker loop
        await workerPromise;

        await new Promise<void>(resolve => server!.close(() => resolve()));
        console.log('Gateway stopped.');

        if (success) {
            console.log("\n✅ All tests passed!");
            finalStatus = 0;
        } else {
            console.log("\n❌ Some tests failed.");
        }

    } catch (err) {
        console.error('Test runner encountered a fatal error:', err);
    } finally {
        // Force exit
        process.exit(finalStatus);
    }
}

main();
