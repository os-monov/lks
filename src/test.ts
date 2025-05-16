import axios from 'axios';

const SERVER_URL = 'http://localhost:8123';
const PARTITIONS = 10;
const RECORD_INTERVAL_MS = 10; // 1 second

async function produce(partitionId) {
  const payload = {
    key: 'key',
    value: 'value',
  };

  try {
    await axios.post(`${SERVER_URL}/produce/${partitionId}`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });
    console.log('Sent record');
  } catch (err) {
    console.log(`Error: ${err.response?.status} ${err.response?.statusText}`);
    console.log(`Error details: ${JSON.stringify(err.response?.data)}`);
    console.log(`[${new Date()}] Failed to send record.`);
  }
}

function producer(partitionId) {
  setInterval(async () => {
    await produce(partitionId);
  }, RECORD_INTERVAL_MS);
}

function start() {
  console.log('Starting to produce records...');
  console.log(`Kafka server has ${PARTITIONS} partitions.`);

  for (let i = 0; i < PARTITIONS; i++) {
    producer(i);
  }
}

start();
