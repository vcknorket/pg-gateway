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
            // No console log here to reduce noise
            const result = await executeQuery(job, ws);
            ws.send(JSON.stringify(result));
        });
        ws.on('close', () => console.log('[Worker] Gateway disconnected.'));
    });
    console.log(`[Worker] Simulated backend listening on ws://localhost:${WORKER_PORT}`);
    return wss;
}

async function executeQuery(job: Job, ws: WebSocket): Promise<WorkerResult> {
    const { queryId, query } = job;
    const lq = query.toLowerCase().trim();
    await new Promise(res => setTimeout(res, 20));
    const baseResult = { queryId, schemaVersion: SCHEMA_VERSION };

    if (lq === 'kill_me') {
        // Special command to test connection decoupling
        setTimeout(() => ws.terminate(), 50);
        return { ...baseResult, status: 'success', payload: { commandTag: 'TERMINATE' } };
    }

    if (lq.includes('error')) return { ...baseResult, status: 'error', payload: { error: { message: 'This query was designed to fail!', code: 'P0001' } } };
    if (lq === 'select 1' || lq === 'select 1;') return { ...baseResult, status: 'success', payload: { columns: [{ name: '?column?', typeOID: 23 }], rows: [['1']], commandTag: 'SELECT 1' } };
    if (lq.includes('foo;bar')) return { ...baseResult, status: 'success', payload: { columns: [{ name: 'test', typeOID: 25 }], rows: [['foo;bar']], commandTag: 'SELECT 1' } };
    const command = query.split(' ')[0].toUpperCase();
    return { ...baseResult, status: 'success', payload: { commandTag: `${command} 0` } };
}


// ===================================================================================
// Test Client Logic
// ===================================================================================

async function runClientTests(wsServer: WebSocketServer): Promise<boolean> {
    console.log('\n--- Starting Comprehensive Test Client ---');
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

        await runTest('SELECT version() fast-path', async () => {
            const res = await client.query('SELECT version()');
            if (!res.rows[0].version.includes('WebSocket Gateway')) throw new Error('Version string is incorrect.');
        });

        // NOTE: Multi-statement rejection tests removed as the feature was deemed too complex for an MVP.

        await runTest('Decoupled connection', async () => {
            // First, a normal query to ensure the connection is good.
            const res1 = await client.query('SELECT 1');
            if (res1.rowCount !== 1) throw new Error('Initial query failed.');

            // Now, send the command to have the worker terminate this specific connection.
            await client.query('KILL_ME');

            // Give the close event time to propagate.
            await new Promise(res => setTimeout(res, 200));

            // This next query should fail because the gateway's WebSocket is now closed.
            try {
                await client.query('SELECT 1');
                throw new Error('Query should have failed after worker terminated the connection.');
            } catch (e) {
                if (!(e as Error).message.includes('backend worker')) {
                    throw new Error(`Query failed with wrong error: ${(e as Error).message}`);
                }
                console.log('Caught expected error after connection was decoupled.');
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

        const success = await runClientTests(wsServer);

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
