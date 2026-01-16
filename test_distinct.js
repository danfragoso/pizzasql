const http = require('http');

async function query(sql) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({ sql });
    const options = {
      hostname: 'localhost',
      port: 8080,
      path: '/query',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function test() {
  console.log('Testing DISTINCT implementation...\n');

  // Create table
  console.log('1. Creating table...');
  try {
    await query('DROP TABLE test_distinct');
  } catch (e) {
    // Ignore error if table doesn't exist
  }
  await query('CREATE TABLE test_distinct (id INTEGER, status TEXT)');
  console.log('   ✓ Table created\n');

  // Insert data
  console.log('2. Inserting data...');
  await query("INSERT INTO test_distinct VALUES (1, 'pending')");
  await query("INSERT INTO test_distinct VALUES (2, 'completed')");
  await query("INSERT INTO test_distinct VALUES (3, 'pending')");
  await query("INSERT INTO test_distinct VALUES (4, 'shipped')");
  await query("INSERT INTO test_distinct VALUES (5, 'pending')");
  await query("INSERT INTO test_distinct VALUES (6, 'completed')");
  console.log('   ✓ Inserted 6 rows\n');

  // Query without DISTINCT
  console.log('3. SELECT status FROM test_distinct:');
  const result1 = await query('SELECT status FROM test_distinct ORDER BY status');
  console.log(`   Rows: ${result1.rows.length}`);
  console.log('   Values:', result1.rows.map(r => r[0]).join(', '));
  console.log();

  // Query with DISTINCT
  console.log('4. SELECT DISTINCT status FROM test_distinct:');
  const result2 = await query('SELECT DISTINCT status FROM test_distinct ORDER BY status');
  console.log(`   Rows: ${result2.rows.length}`);
  console.log('   Values:', result2.rows.map(r => r[0]).join(', '));
  console.log();

  // Verify
  if (result2.rows.length === 3) {
    console.log('✅ DISTINCT is working correctly!');
  } else {
    console.log(`❌ DISTINCT failed - expected 3 unique values, got ${result2.rows.length}`);
  }

  // Clean up
  await query('DROP TABLE test_distinct');
}

test().catch(console.error);
