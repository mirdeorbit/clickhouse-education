import dotenv from 'dotenv';
dotenv.config();

const CITIES = ['Moscow', 'Saint Petersburg', 'Kazan', 'Novosibirsk', 'Yekaterinburg'];
const ORDER_STATUS = ['created', 'collecting', 'collected', 'delivering', 'delivered', 'completed'];

function randomCity() {
  const cities = CITIES;
  return cities[Math.floor(Math.random() * cities.length)];
}

function formatDateToClickHouse(date) {
  const isoString = date.toISOString();
  return isoString.replace('T', ' ').replace('Z', '').replace(/\.(\d{3})Z$/, '.$1');
}

function randomDateInRange(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function addRandomMinutes(date, minMinutes, maxMinutes) {
  const minutes = minMinutes + Math.random() * (maxMinutes - minMinutes);
  return new Date(date.getTime() + minutes * 60 * 1000);
}

function generateOrderData(baseId, city, status, shopId, clientId, pickerId, courierId, payment, startDate) {
  const createDate = randomDateInRange(startDate, new Date(startDate.getTime() + 86400000 * 60));

  let payDate = null;
  let collectingStart = null;
  let collectingEnd = null;
  let courierAssigned = null;
  let courierTake = null;
  let courierDelivered = null;
  let completed = null;

  if (payment && Math.random() > 0.3) {
    payDate = addRandomMinutes(createDate, 5, 60);
  }

  let currentDate = createDate;

  switch (status) {
    case 'collecting':
      collectingStart = addRandomMinutes(currentDate, 5, 30);
      break;

    case 'collected':
      collectingStart = addRandomMinutes(currentDate, 5, 30);
      collectingEnd = addRandomMinutes(collectingStart, 10, 45);
      if (!payDate && payment && Math.random() > 0.3) {
        payDate = randomDateInRange(collectingStart, collectingEnd);
      }
      break;

    case 'delivering':
      collectingStart = addRandomMinutes(currentDate, 5, 30);
      collectingEnd = addRandomMinutes(collectingStart, 10, 45);
      courierAssigned = addRandomMinutes(collectingEnd, 5, 20);
      courierTake = addRandomMinutes(courierAssigned, 5, 25);
      if (!payDate && payment && Math.random() > 0.3) {
        payDate = randomDateInRange(collectingEnd, courierTake);
      }
      break;

    case 'delivered':
      collectingStart = addRandomMinutes(currentDate, 5, 30);
      collectingEnd = addRandomMinutes(collectingStart, 10, 45);
      courierAssigned = addRandomMinutes(collectingEnd, 5, 20);
      courierTake = addRandomMinutes(courierAssigned, 5, 25);
      courierDelivered = addRandomMinutes(courierTake, 15, 60);
      if (!payDate && payment && Math.random() > 0.3) {
        payDate = randomDateInRange(courierTake, courierDelivered);
      }
      break;

    case 'completed':
      collectingStart = addRandomMinutes(currentDate, 5, 30);
      collectingEnd = addRandomMinutes(collectingStart, 10, 45);
      courierAssigned = addRandomMinutes(collectingEnd, 5, 20);
      courierTake = addRandomMinutes(courierAssigned, 5, 25);
      courierDelivered = addRandomMinutes(courierTake, 15, 60);
      completed = addRandomMinutes(courierDelivered, 5, 30);
      if (!payDate && payment) {
        payDate = randomDateInRange(createDate, completed);
      }
      break;

    case 'created':
    default:
      if (!payDate && payment && Math.random() > 0.5) {
        payDate = addRandomMinutes(createDate, 5, 30);
      }
      break;
  }

  const now = Date.now();

  let row = {
    id: baseId,
    city: city,
    status: status,
    shop_id: shopId,
    client_id: clientId,
    picker_id: pickerId,
    courier_id: courierId,
    payment: payment ? 1 : 0,
    create_date: formatDateToClickHouse(createDate),
    pay_date: payDate ? formatDateToClickHouse(payDate) : null,
    collecting_start_date: collectingStart ? formatDateToClickHouse(collectingStart) : null,
    collecting_end_date: collectingEnd ? formatDateToClickHouse(collectingEnd) : null,
    courier_assigned_date: courierAssigned ? formatDateToClickHouse(courierAssigned) : null,
    courier_take_date: courierTake ? formatDateToClickHouse(courierTake) : null,
    courier_delivered_date: courierDelivered ? formatDateToClickHouse(courierDelivered) : null,
    completed_date: completed ? formatDateToClickHouse(completed) : null,
    is_deleted: 0,
    ts_ms: now
  };

  return row;
}

async function insertBatchToClickHouse(host, username, password, database, batch) {
  const url = new URL(host);
  url.searchParams.set('query', `INSERT INTO ods.delivery_orders FORMAT JSONEachRow`);
  url.searchParams.set('database', database);

  const jsonData = batch.map(row => JSON.stringify(row)).join('\n');

  const headers = {
    'Content-Type': 'application/json'
  };

  if (username && username !== 'default') {
    headers['X-ClickHouse-User'] = username;
  }

  if (password) {
    headers['X-ClickHouse-Key'] = password;
  }

  const response = await fetch(url.toString(), {
    method: 'POST',
    headers: headers,
    body: jsonData
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`ClickHouse error: ${response.status} ${response.statusText} - ${text}`);
  }

  const resText = await response.text();
  return {
    data: resText || 'OK'
  };
}

async function seedClickHouseOrders({
  host = 'http://localhost:8123',
  username = 'default',
  password = '',
  database = 'ods',
  batchSize = 100000,
  numOrders = 10000000,
  numShops = 100000,
  numClients = 100000,
  numPickers = 100000,
  numCouriers = 100000,
  startDate = '2024-01-01'
}) {
  console.log('Starting to generate and insert orders...');
  console.log(`Config: ${numOrders} orders, batch size: ${batchSize}`);

  let totalInserted = 0;
  let batch = [];
  let batchNum = 0;

  const startDateObj = new Date(startDate);

  for (let i = 0; i < numOrders; i++) {
    const orderId = i + 1;
    const city = randomCity();
    const status = ORDER_STATUS[Math.floor(Math.random() * ORDER_STATUS.length)];
    const shopId = Math.floor(Math.random() * numShops) + 1;
    const clientId = Math.floor(Math.random() * numClients) + 1;
    const pickerId = Math.random() > 0.3 ? Math.floor(Math.random() * numPickers) + 1 : null;
    const courierId = Math.random() > 0.3 ? Math.floor(Math.random() * numCouriers) + 1 : null;
    const payment = Math.random() > 0.5;

    const orderData = generateOrderData(orderId, city, status, shopId, clientId, pickerId, courierId, payment, startDateObj);

    batch.push(orderData);

    if (batch.length >= batchSize) {
      try {
        await insertBatchToClickHouse(host, username, password, database, batch);
        batchNum++;
        totalInserted += batch.length;
        const progress = ((totalInserted / numOrders) * 100).toFixed(2);
        console.log(`Batch ${batchNum} inserted: ${batch.length} orders (total: ${totalInserted}, ${progress}%)`);
        batch = [];
      } catch (error) {
        console.error(`Error inserting batch ${batchNum}:`, error.message);
        throw error;
      }
    }
  }

  if (batch.length > 0) {
    try {
      await insertBatchToClickHouse(host, username, password, database, batch);
      batchNum++;
      totalInserted += batch.length;
      const progress = ((totalInserted / numOrders) * 100).toFixed(2);
      console.log(`Final batch ${batchNum} inserted: ${batch.length} orders (total: ${totalInserted}, ${progress}%)`);
    } catch (error) {
      console.error(`Error inserting final batch:`, error.message);
      throw error;
    }
  }

  console.log(`Completed! Total orders inserted: ${totalInserted}`);
}

seedClickHouseOrders({
  host: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DATABASE || 'ods',
  batchSize: 100000,
  numOrders: 10000000,
  numShops: 100000,
  numClients: 100000,
  numPickers: 100000,
  numCouriers: 100000,
  startDate: '2024-01-01'
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
