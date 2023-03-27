require("dotenv").config();
const { Client } = require("cassandra-driver");

async function run() {
  console.log(`Starting`);
  const client = new Client({
    cloud: {
      secureConnectBundle: "../secure-connect-astra-aws-sp.zip",
    },
    credentials: {
      username: process.env.ASTRA_CLIENT_ID,
      password: process.env.ASTRA_CLIENT_SECRET,
    },
  });

  await client.connect();
  console.log(`Connected`);
  let a = 1;

  while (a < 1000) {
    const rs = await client.execute(
      `INSERT INTO app.mytab (a,b) Values (${a},${Math.trunc(
        Math.random() * 100
      )})`
    );
    console.log(`Insert ${a} `);
    a += 1;
    await sleep(100);
  }
  // Execute a query
  const rs = await client.execute("SELECT * FROM app.mytab");
  console.log(`Your cluster returned ${rs.rowLength} row(s)`);
  console.table(rs.rows.slice(0, 10));

  await client.shutdown();
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// Run the async function
run();
