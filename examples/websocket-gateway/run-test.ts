import { Server } from 'node:net';
import { createGateway, Job, WorkerResult } from './index.js';
import { WebSocketServer, WebSocket } from 'ws';
import { Client } from 'pg';

// ===================================================================================
//
// NOTICE: This file combines a simulated backend, the gateway, and a test client
// into a single script. This is a workaround for an unstable execution environment
// that makes multi-process testing difficult.
//
// ===================================================================================


// --- Configuration ---
const PG_PORT = 5432;
const WORKER_PORT = 8080;
const CONNECTION_STRING = `postgresql://testuser:testpassword@localhost:${PG_PORT}/testdb`;


// ===================================================================================
// Simulated Worker Backend Logic
// ===================================================================================

/**
 * Creates and starts a WebSocket server that simulates the backend worker.
 * @returns The WebSocketServer instance.
 */
function createWorker(): WebSocketServer {
    const wss = new WebSocketServer({ port: WORKER_PORT });

    wss.on('connection', (ws) => {
        console.log('[Worker] Gateway connected.');

        ws.on('message', async (data) => {
            const job: Job = JSON.parse(data.toString());
            console.log(`[Worker] Received job ${job.queryId}: "${job.query}"`);

            const result = await executeQuery(job);

            ws.send(JSON.stringify(result));
            console.log(`[Worker] Sent result for ${job.queryId}`);
        });

        ws.on('close', () => {
            console.log('[Worker] Gateway disconnected.');
        });
    });

    console.log(`[Worker] Simulated backend listening on ws://localhost:${WORKER_PORT}`);
    return wss;
}

/**
 * Simulates executing a query against a data engine like Trino.
 * @param job The query job from the gateway.
 * @returns A result payload.
 */
async function executeQuery(job: Job): Promise<WorkerResult> {
    const { queryId, query } = job;
    const lq = query.toLowerCase().trim();

    // Simulate async work
    await new Promise(res => setTimeout(res, 50));

    if (lq.includes('error')) {
        return { queryId, status: 'error', payload: { error: { message: 'This query was designed to fail!', code: 'P0001' } } };
    }

    if (lq === 'select 1' || lq === 'select 1;') {
        return { queryId, status: 'success', payload: {
            columns: [{ name: '?column?', typeOID: 23 }], // int4
            rows: [['1']],
            commandTag: 'SELECT 1'
        }};
    }

    if (lq.includes('from users')) {
        return { queryId, status: 'success', payload: {
            columns: [
                { name: 'id', typeOID: 23 },
                { name: 'name', typeOID: 25 }, // text
                { name: 'email', typeOID: 25 },
            ],
            rows: [
                ['1', 'Alice', 'alice@example.com'],
                ['2', 'Bob', 'bob@example.com'],
                ['3', 'Charlie', null], // Add a NULL to test handling
            ],
            commandTag: 'SELECT 3'
        }};
    }

    // Default for DDL/DML-like commands
    const command = query.split(' ')[0].toUpperCase();
    return { queryId, status: 'success', payload: { commandTag: `${command} 0` } };
}


// ===================================================================================
// Test Client Logic
// ===================================================================================

async function runClientTests(): Promise<boolean> {
    console.log('\n--- Starting Test Client ---');
    const client = new Client({ connectionString: CONNECTION_STRING });
    let testSuccess = true;

    try {
        await client.connect();
        console.log('Client connected to gateway.');

        // Test 1: Simple literal select
        console.log('\nRunning test: SELECT 1');
        const res1 = await client.query('SELECT 1;');
        if (res1.rows[0]['?column?'] !== 1) throw new Error(`Test 1 Failed: Expected 1, got ${res1.rows[0]['?column?']}`);
        console.log('Test 1 Passed.');

        // Test 2: Simulated table select with a NULL value
        console.log('\nRunning test: SELECT * FROM users');
        const res2 = await client.query('SELECT * FROM users;');
        if (res2.rowCount !== 3) throw new Error(`Test 2 Failed: Expected 3 rows, got ${res2.rowCount}`);
        if (res2.rows[2].email !== null) throw new Error(`Test 2 Failed: Expected NULL value, got ${res2.rows[2].email}`);
        console.log('Test 2 Passed.');
        console.table(res2.rows);


        // Test 3: Query that should produce an error
        console.log('\nRunning test: Error query');
        try {
            await client.query('select from error');
            throw new Error('Test 3 Failed: Query did not produce an error as expected.');
        } catch (e) {
            if(!(e as Error).message.includes('designed to fail')) throw new Error(`Test 3 Failed: Wrong error message: ${(e as Error).message}`);
            console.log('Test 3 Passed (Caught expected error).');
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
    let pgServer: Server | null = null;
    let wsServer: WebSocketServer | null = null;
    let finalStatus = 1;

    try {
        // 1. Start Servers
        wsServer = createWorker();
        pgServer = createGateway();
        await new Promise<void>(resolve => pgServer!.listen(PG_PORT, resolve));

        // 2. Run Client
        const success = await runClientTests();

        if (success) {
            console.log("\n✅ All tests passed!");
            finalStatus = 0;
        } else {
            console.log("\n❌ Some tests failed.");
        }

    } catch (err) {
        console.error('Test runner encountered a fatal error:', err);
    } finally {
        // 3. Teardown
        console.log('\nShutting down...');
        if (wsServer) {
            wsServer.close();
            // Close all client connections
            for (const ws of wsServer.clients) {
                ws.terminate();
            }
        }
        if (pgServer) {
            await new Promise<void>(resolve => pgServer!.close(() => resolve()));
        }
        console.log('Shutdown complete.');
        process.exit(finalStatus);
    }
}

main();
