import { Server } from 'node:net';
import { createGateway, Job, WorkerResult } from './index.js';
import { WebSocketServer, WebSocket } from 'ws';
import { Client } from 'pg';

// ===================================================================================
//
//  This file contains the robust, integrated test runner for the WebSocket gateway.
//
// ===================================================================================


// --- Configuration ---
const PG_PORT = parseInt(process.env.PG_PORT || '5432', 10);
const WORKER_PORT = 8080;
const CONNECTION_STRING = `postgresql://testuser:testpassword@localhost:${PG_PORT}/testdb`;
const SCHEMA_VERSION = '1.0.0';


// ===================================================================================
// Simulated Worker Backend Logic
// ===================================================================================

function createWorker(): WebSocketServer {
    const wss = new WebSocketServer({ port: WORKER_PORT });
    wss.on('connection', (ws) => {
        console.log('[Worker] Gateway connected.');
        ws.on('message', async (data) => {
            const job: Job = JSON.parse(data.toString());
            console.log(`[Worker] Received job ${job.queryId}: "${job.query}"`);
            const result = await executeQuery(job);
            // Simulate a slow worker for timeout tests
            if (job.query.includes('slow_query')) {
                await new Promise(res => setTimeout(res, 200));
            }
            ws.send(JSON.stringify(result));
            console.log(`[Worker] Sent result for ${job.queryId}`);
        });
    });
    console.log(`[Worker] Simulated backend listening on ws://localhost:${WORKER_PORT}`);
    return wss;
}

async function executeQuery(job: Job): Promise<WorkerResult> {
    const { queryId, query } = job;
    const lq = query.toLowerCase().trim();

    await new Promise(res => setTimeout(res, 20)); // Simulate base latency

    const baseResult = { queryId, schemaVersion: SCHEMA_VERSION };

    if (lq.includes('error')) return { ...baseResult, status: 'error', payload: { error: { message: 'This query was designed to fail!', code: 'P0001' } } };
    if (lq === 'select 1' || lq === 'select 1;') return { ...baseResult, status: 'success', payload: { columns: [{ name: '?column?', typeOID: 23 }], rows: [['1']], commandTag: 'SELECT 1' } };
    if (lq.includes('empty_table')) return { ...baseResult, status: 'success', payload: { columns: [{ name: 'id', typeOID: 23 }], rows: [], commandTag: 'SELECT 0' } };
    if (lq.includes('users')) return { ...baseResult, status: 'success', payload: { columns: [{ name: 'id', typeOID: 23 }, { name: 'name', typeOID: 25 }], rows: [['1', 'Alice'], ['2', 'Bob']], commandTag: 'SELECT 2' } };

    const command = query.split(' ')[0].toUpperCase();
    return { ...baseResult, status: 'success', payload: { commandTag: `${command} 0` } };
}


// ===================================================================================
// Test Client Logic
// ===================================================================================

async function runClientTests(): Promise<boolean> {
    console.log('\n--- Starting Comprehensive Test Client ---');
    const client = new Client({ connectionString: CONNECTION_STRING }); // Let the gateway handle timeouts
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
            const res = await client.query('SELECT 1;');
            if (res.rows[0]['?column?'] !== 1) throw new Error(`Expected 1, got ${res.rows[0]['?column?']}`);
        });

        await runTest('Empty Result Set', async () => {
            const res = await client.query('select * from empty_table');
            if (res.rowCount !== 0) throw new Error(`Expected 0 rows, got ${res.rowCount}`);
            if (res.fields[0].name !== 'id') throw new Error(`Expected column 'id', got ${res.fields[0].name}`);
        });

        await runTest('Multi-statement rejection', async () => {
            try {
                await client.query('SELECT 1; SELECT 2;');
                throw new Error('Should have rejected multi-statement query.');
            } catch (e) {
                if (!(e as Error).message.includes('Multi-statement')) throw new Error(`Wrong error message: ${(e as Error).message}`);
            }
        });

        await runTest('Query Timeout', async () => {
             try {
                // This client has a 100ms timeout, worker has 200ms delay
                await client.query('select * from slow_query');
                throw new Error('Query should have timed out.');
            } catch (e) {
                // The error comes from the gateway's timeout, not the client's statement_timeout
                if (!(e as Error).message.includes('Timed out waiting for worker')) throw new Error(`Wrong error message: ${(e as Error).message}`);
            }
        });

        await runTest('Error from worker', async () => {
            try {
                await client.query('select from error');
                throw new Error('Query did not produce an error as expected.');
            } catch (e) {
                if(!(e as Error).message.includes('designed to fail')) throw new Error(`Wrong error message: ${(e as Error).message}`);
            }
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
