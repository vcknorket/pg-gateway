import { Client } from 'pg';

const CONNECTION_STRING = 'postgresql://testuser:testpassword@localhost:5432/testdb';

async function runTest(client: Client, name: string, query: string) {
    console.log(`\n--- Running Test: ${name} ---`);
    console.log(`Query: ${query}`);
    try {
        const res = await client.query(query);
        console.log('Result:');
        console.table(res.rows);
    } catch (err) {
        console.error('Caught expected error:', err.message);
    }
}

async function main() {
    console.log('Starting test client...');
    const client = new Client({ connectionString: CONNECTION_STRING });

    try {
        await client.connect();
        console.log('Connected to gateway successfully.');

        // Test 1: Simple literal select
        await runTest(client, 'Simple Select', 'SELECT 1;');

        // Test 2: Simulated table select
        await runTest(client, 'Table Select', 'SELECT * FROM users;');

        // Test 3: Query that should produce an error
        await runTest(client, 'Error Query', 'SELECT * FROM error_table;');

        // Test 4: Query that should return an empty result
        await runTest(client, 'Empty Result Query', 'CREATE TABLE foo (id int);');

        // Test 5: Built-in version() query
        await runTest(client, 'Version Query', 'SELECT version()');

        console.log('\n--- All tests completed ---');

    } catch (err) {
        console.error('Test client failed:', err);
    } finally {
        await client.end();
        console.log('Disconnected from gateway.');
    }
}

main();
