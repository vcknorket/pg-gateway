import { Server } from 'node:net';
import { createGateway, Job, WorkerMessage } from './index.js';
import { WebSocketServer, WebSocket } from 'ws';
import { Client } from 'pg';
import { randomUUID } from 'node:crypto';

// ===================================================================================
//
//  This file contains the test runner for the Streaming WebSocket Gateway.
//
// ===================================================================================


// --- Configuration ---
const PG_PORT = 5432;
const WORKER_PORT = 8080;
const CONNECTION_STRING = `postgresql://testuser:testpassword@localhost:${PG_PORT}/testdb`;


// ===================================================================================
// Simulated Worker Backend Logic (Streaming Protocol)
// ===================================================================================

function createWorker(): WebSocketServer {
    const wss = new WebSocketServer({ port: WORKER_PORT });
    wss.on('connection', (ws) => {
        console.log('[Worker] Gateway connected.');
        ws.on('message', async (data) => {
            const job: Job = JSON.parse(data.toString());
            console.log(`[Worker] Received query: "${job.query}"`);
            // The worker now generates the query_id
            const query_id = randomUUID();
            await executeQuery(job, ws, query_id);
        });
    });
    console.log(`[Worker] Simulated backend listening on ws://localhost:${WORKER_PORT}`);
    return wss;
}

async function executeQuery(job: Job, ws: WebSocket, query_id: string) {
    const { query } = job;
    const lq = query.toLowerCase().trim();

    // --- Test Case: Multi-batch response ---
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
    const errorMsg: WorkerMessage = { query_id, type: 'error', payload: { message: 'Unsupported query for test.', code: 'XX000' } };
    ws.send(JSON.stringify(errorMsg));
}


// ===================================================================================
// Test Client Logic
// ===================================================================================

async function runClientTests(): Promise<boolean> {
    console.log('\n--- Starting Test Client for Streaming Protocol ---');
    const client = new Client({ connectionString: CONNECTION_STRING });
    let testSuccess = true;

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
            if (res.rows[2].batch_no !== 3) throw new Error(`Incorrect data in last row: ${res.rows[2].batch_no}`);
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

async function main() {
    let pgServer: Server | null = null;
    let wsServer: WebSocketServer | null = null;
    let finalStatus = 1;

    try {
        wsServer = createWorker();
        pgServer = createGateway();
        await new Promise<void>(resolve => pgServer!.listen(PG_PORT, resolve));

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
        console.log('\nShutting down...');
        wsServer?.close();
        if (pgServer) await new Promise<void>(resolve => pgServer!.close(() => resolve()));
        console.log('Shutdown complete.');
        process.exit(finalStatus);
    }
}

main();
