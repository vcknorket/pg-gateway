/**
 * @file This file contains a self-contained, integrated test runner for the
 * Streaming WebSocket Gateway.
 *
 * It is designed to be run as a single script and performs the following actions:
 * 1. Starts a mock WebSocket server that simulates the backend worker.
 * 2. Starts the actual PostgreSQL gateway server from `index.ts`.
 * 3. Starts a PostgreSQL client that connects to the gateway.
 * 4. Runs a series of tests to validate the end-to-end functionality.
 * 5. Shuts down all components and exits with a status code indicating success or failure.
 *
 * This approach was chosen to provide reliable testing in an environment where
 * managing multiple independent processes is difficult.
 */

import { Server } from 'node:net';
import { createGateway, Job, WorkerMessage } from './index.js';
import { WebSocketServer, WebSocket } from 'ws';
import { Client } from 'pg';
import { randomUUID } from 'node:crypto';

// --- Configuration ---
const PG_PORT = 5432;
const WORKER_PORT = 8080;
const CONNECTION_STRING = `postgresql://testuser:testpassword@localhost:${PG_PORT}/testdb`;


// ===================================================================================
// Simulated Worker Backend Logic (Streaming Protocol)
// ===================================================================================

/**
 * Creates and starts a mock WebSocket server.
 * This server simulates the behavior of the real backend worker, speaking the
 * custom streaming protocol defined in `index.ts`.
 * @returns An instance of a WebSocketServer.
 */
function createWorker(): WebSocketServer {
    const wss = new WebSocketServer({ port: WORKER_PORT });
    wss.on('connection', (ws) => {
        console.log('[Worker] Gateway connected.');
        ws.on('message', async (data) => {
            const job: Job = JSON.parse(data.toString());
            console.log(`[Worker] Received query: "${job.query}"`);
            // The worker is responsible for generating the query_id for tracking.
            const query_id = randomUUID();
            await executeQuery(job, ws, query_id);
        });
    });
    console.log(`[Worker] Simulated backend listening on ws://localhost:${WORKER_PORT}`);
    return wss;
}

/**
 * Simulates the execution of a SQL query.
 * Based on the query string, it sends a sequence of `schema`, `data`, and `complete`
 * messages back to the gateway over the provided WebSocket, mimicking a real,
 * streaming database engine.
 * @param job The query job received from the gateway.
 * @param ws The specific WebSocket connection to send the response on.
 * @param query_id The unique ID for this query execution.
 */
async function executeQuery(job: Job, ws: WebSocket, query_id: string) {
    const { query } = job;
    const lq = query.toLowerCase().trim();

    // --- Test Case: Multi-batch response ---
    // This case tests the gateway's ability to handle multiple 'data' messages for a single query.
    if (lq.includes('multi_batch')) {
        // 1. Send Schema
        const schemaMsg: WorkerMessage = { query_id, type: 'schema', payload: { columns: [{ name: 'batch_no', typeOID: 23 }] } };
        ws.send(JSON.stringify(schemaMsg));
        await new Promise(res => setTimeout(res, 20));

        // 2. Send Data Batch 1
        const dataMsg1: WorkerMessage = { query_id, type: 'data', payload: [['1'], ['2']] };
        ws.send(JSON.stringify(dataMsg1));
        await new Promise(res => setTimeout(res, 20));

        // 3. Send Data Batch 2
        const dataMsg2: WorkerMessage = { query_id, type: 'data', payload: [['3']] };
        ws.send(JSON.stringify(dataMsg2));
        await new Promise(res => setTimeout(res, 20));

        // 4. Send Complete
        const completeMsg: WorkerMessage = { query_id, type: 'complete', payload: { commandTag: 'SELECT 3', total_rows: 3 } };
        ws.send(JSON.stringify(completeMsg));
        return;
    }

    // --- Test Case: Simple SELECT ---
    // This case tests the basic success path.
    if (lq === 'select 1') {
        const schemaMsg: WorkerMessage = { query_id, type: 'schema', payload: { columns: [{ name: '?column?', typeOID: 23 }] } };
        ws.send(JSON.stringify(schemaMsg));
        await new Promise(res => setTimeout(res, 10));
        const dataMsg: WorkerMessage = { query_id, type: 'data', payload: [['1']] };
        ws.send(JSON.stringify(dataMsg));
        await new Promise(res => setTimeout(res, 10));
        const completeMsg: WorkerMessage = { query_id, type: 'complete', payload: { commandTag: 'SELECT 1', total_rows: 1 } };
        ws.send(JSON.stringify(completeMsg));
        return;
    }

    // --- Test Case: Error ---
    // This case tests the gateway's ability to handle errors from the worker.
    const errorMsg: WorkerMessage = { query_id, type: 'error', payload: { message: 'Unsupported query for test.', code: 'XX000' } };
    ws.send(JSON.stringify(errorMsg));
}


// ===================================================================================
// Test Client Logic
// ===================================================================================

/**
 * Runs a suite of tests against the gateway using a standard `pg` client.
 * @returns A promise that resolves to `true` if all tests pass, `false` otherwise.
 */
async function runClientTests(): Promise<boolean> {
    console.log('\n--- Starting Test Client for Streaming Protocol ---');
    const client = new Client({ connectionString: CONNECTION_STRING });
    let testSuccess = true;

    /** A helper to wrap individual test cases for clear logging and error handling. */
    const runTest = async (name: string, fn: () => Promise<void>) => {
        try {
            console.log(`\n--- Running test: ${name} ---`);
            await fn();
            console.log(`--- Test PASSED: ${name} ---`);
        } catch (err) {
            console.error(`--- Test FAILED: ${name} ---`);
            console.error(err);
            testSuccess = false;
        }
    };

    try {
        await client.connect();
        console.log('Client connected to gateway.');

        await runTest('Simple Select', async () => {
            const res = await client.query('SELECT 1');
            if (res.rows[0]['?column?'] !== 1) throw new Error(`Expected 1, got ${res.rows[0]['?column?']}`);
            if (res.command !== 'SELECT' || res.rowCount !== 1) throw new Error(`Incorrect command tag or row count.`);
        });

        await runTest('Multi-batch streamed response', async () => {
            const res = await client.query('select * from multi_batch');
            if (res.rowCount !== 3) throw new Error(`Expected 3 rows, got ${res.rowCount}`);
            // The pg client reassembles the batches, so we can test the final result.
            if (Number(res.rows[2].batch_no) !== 3) throw new Error(`Incorrect data in last row: ${res.rows[2].batch_no}`);
        });

    } catch (err) {
        console.error('Test client failed to connect or run setup.');
        testSuccess = false;
    } finally {
        await client.end();
        return testSuccess;
    }
}


// ===================================================================================
// Main Test Runner
// ===================================================================================

/**
 * The main function that orchestrates the entire test run.
 */
async function main() {
    let pgServer: Server | null = null;
    let wsServer: WebSocketServer | null = null;
    let finalStatus = 1; // Default to fail

    try {
        // 1. Start the mock worker and the gateway
        wsServer = createWorker();
        pgServer = createGateway();
        await new Promise<void>(resolve => pgServer!.listen(PG_PORT, resolve));

        // 2. Run the client tests
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
        // 3. Gracefully shut down all components
        console.log('\nShutting down...');
        wsServer?.close();
        if (pgServer) await new Promise<void>(resolve => pgServer!.close(() => resolve()));
        console.log('Shutdown complete.');
        process.exit(finalStatus);
    }
}

main();
