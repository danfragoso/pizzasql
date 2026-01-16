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
      },
      timeout: 10000 // 10 second timeout
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        try {
          const result = JSON.parse(body);
          if (result.error) {
            reject(new Error(result.error.message));
          } else {
            resolve(result);
          }
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
    req.write(data);
    req.end();
  });
}

async function testDistinct() {
  try {
    console.log('Testing DISTINCT implementation...\n');

    // Create table
    console.log('1. Creating table...');
    await query('CREATE TABLE test_distinct (id INTEGER, status TEXT)');
    console.log('   ✓ Table created\n');

    // Insert data
    console.log('2. Inserting data...');
    for (let i = 0; i < 10; i++) {
      const statuses = ['pending', 'completed', 'shipped'];
      const status = statuses[i % 3];
      await query(`INSERT INTO test_distinct VALUES (${i}, '${status}')`);
    }
    console.log('   ✓ Inserted 10 rows\n');

    // Query without DISTINCT
    console.log('3. SELECT status FROM test_distinct:');
    const result1 = await query('SELECT status FROM test_distinct ORDER BY status');
    console.log(`   Rows: ${result1.rows.length}`);
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
      console.log(`   Expected 3 unique values, got ${result2.rows.length}`);
    } else {
      console.log(`❌ DISTINCT failed - expected 3 unique values, got ${result2.rows.length}`);
    }

    // Clean up
    console.log('\n5. Cleaning up...');
    await query('DROP TABLE test_distinct');
    console.log('   ✓ Table dropped');

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

testDistinct();
