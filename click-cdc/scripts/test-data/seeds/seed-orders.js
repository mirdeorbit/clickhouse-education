// seed-orders.js
import pg from 'pg';
import { faker } from '@faker-js/faker';
import dotenv from 'dotenv';
dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

const CITIES = ['Moscow', 'Saint Petersburg', 'Kazan', 'Novosibirsk', 'Yekaterinburg'];
const ORDER_STATUS = [/*'created', 'collecting',*/ 'collected', 'delivering', 'delivered', 'completed'];

const randomCity = () => faker.helpers.arrayElement(CITIES);
const randomString = (length = 8) => faker.string.alpha({ count: length, casing: 'mixed' }).toUpperCase();

async function getIds(client, tableName) {
  const res = await client.query(`SELECT id FROM ${tableName}`);
  return res.rows.map(row => row.id);
}

async function ensurePolygons(client, numPolygons = 3) {
  const countRes = await client.query(`SELECT COUNT(*) FROM polygons`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numPolygons) {
    const existing = await getIds(client, 'polygons');
    return existing;
  }

  const toCreate = numPolygons - existingCount;
  console.log(`Need to create ${toCreate} polygons`);
  const BATCH_SIZE = 10000;
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 2 + 1}, $${i * 2 + 2})`);
      params.push(randomString(10), randomCity());
    }
    const res = await client.query(
      `INSERT INTO polygons (name, city) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} polygons`);
  const allPolygons = await getIds(client, 'polygons');
  return allPolygons;
}

async function ensureShops(client, polygonIds, numShops = 5) {
  const countRes = await client.query(`SELECT COUNT(*) FROM shops`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numShops) {
    const existing = await getIds(client, 'shops');
    return existing;
  }

  const toCreate = numShops - existingCount;
  console.log(`Need to create ${toCreate} shops`);
  const BATCH_SIZE = 1000;
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 5 + 1}, $${i * 5 + 2}, $${i * 5 + 3}, $${i * 5 + 4}, $${i * 5 + 5})`);
      params.push(
        faker.location.streetAddress(),
        randomCity(),
        faker.helpers.arrayElement(polygonIds),
        '08:00',
        '20:00'
      );
    }
    const res = await client.query(
      `INSERT INTO shops (address, city, polygon_id, start_work_time, end_work_time) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} shops`);
  const allShops = await getIds(client, 'shops');
  return allShops;
}

async function ensureClients(client, numClients = 15) {
  const countRes = await client.query(`SELECT COUNT(*) FROM clients`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numClients) {
    const existing = await getIds(client, 'clients');
    return existing;
  }

  const toCreate = numClients - existingCount;
  console.log(`Need to create ${toCreate} clients`);
  const BATCH_SIZE = 1000;
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`);
      params.push(
        faker.person.fullName(),
        faker.phone.number({ style: 'national' }),
        faker.location.streetAddress()
      );
    }
    const res = await client.query(
      `INSERT INTO clients (full_name, phone, address) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} clients`);
  const allClients = await getIds(client, 'clients');
  return allClients;
}

async function ensurePickers(client, shopIds, numPickers = 10) {
  const countRes = await client.query(`SELECT COUNT(*) FROM pickers`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numPickers) {
    const existing = await getIds(client, 'pickers');
    return existing;
  }

  const toCreate = numPickers - existingCount;
  console.log(`Need to create ${toCreate} pickers`);
  const BATCH_SIZE = 1000;
  const pickerStatuses = ['blocked', 'free', 'busy'];
  const networks = ['asx', 'self_delivery', 'otus_logistics'];
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 10 + 1}, $${i * 10 + 2}, $${i * 10 + 3}, $${i * 10 + 4}, $${i * 10 + 5}, $${i * 10 + 6}, $${i * 10 + 7}, $${i * 10 + 8}, $${i * 10 + 9}, $${i * 10 + 10})`);
      params.push(
        faker.person.firstName(),
        faker.person.lastName(),
        faker.person.middleName() || null,
        faker.phone.number({ style: 'national' }),
        faker.internet.email() || null,
        faker.helpers.arrayElement(pickerStatuses),
        faker.helpers.arrayElement(networks),
        randomCity(),
        'Europe/Moscow',
        faker.helpers.maybe(() => faker.helpers.arrayElement(shopIds)) || null
      );
    }
    const res = await client.query(
      `INSERT INTO pickers (first_name, last_name, patronymic, phone, email, status, network, city, timezone, shop_id) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} pickers`);
  const allPickers = await getIds(client, 'pickers');
  return allPickers;
}

async function ensureCouriers(client, polygonIds, numCouriers = 10) {
  const countRes = await client.query(`SELECT COUNT(*) FROM couriers`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numCouriers) {
    const existing = await getIds(client, 'couriers');
    return existing;
  }

  const toCreate = numCouriers - existingCount;
  console.log(`Need to create ${toCreate} couriers`);
  const BATCH_SIZE = 1000;
  const courierStatuses = ['blocked', 'on_work', 'not_on_work'];
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 12 + 1}, $${i * 12 + 2}, $${i * 12 + 3}, $${i * 12 + 4}, $${i * 12 + 5}, $${i * 12 + 6}, $${i * 12 + 7}, $${i * 12 + 8}, $${i * 12 + 9}, $${i * 12 + 10}, $${i * 12 + 11}, $${i * 12 + 12})`);
      params.push(
        faker.person.firstName(),
        faker.person.lastName(),
        faker.person.middleName() || null,
        faker.phone.number({ style: 'national' }),
        faker.internet.email() || null,
        faker.helpers.arrayElement([null, faker.string.numeric(10)]),
        randomCity(),
        faker.helpers.arrayElement(courierStatuses),
        faker.company.name() || null,
        faker.datatype.boolean(),
        'Europe/Moscow',
        faker.helpers.maybe(() => faker.helpers.arrayElement(polygonIds)) || null
      );
    }
    const res = await client.query(
      `INSERT INTO couriers (first_name, last_name, patronymic, phone, email, inn, city, status, company, self_employed, timezone, polygon_id) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} couriers`);
  const allCouriers = await getIds(client, 'couriers');
  return allCouriers;
}

async function ensureProducts(client, numProducts = 30) {
  const countRes = await client.query(`SELECT COUNT(*) FROM products`);
  const existingCount = parseInt(countRes.rows[0].count);
  if (existingCount >= numProducts) {
    const existing = await getIds(client, 'products');
    return existing;
  }

  const toCreate = numProducts - existingCount;
  console.log(`Need to create ${toCreate} products`);
  const BATCH_SIZE = 1000;
  const amountTypes = ['weight', 'items'];
  let createdTotal = 0;

  for (let batchStart = 0; batchStart < toCreate; batchStart += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, toCreate - batchStart);
    const values = [];
    const params = [];
    for (let i = 0; i < batchSize; i++) {
      values.push(`($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`);
      params.push(
        faker.commerce.productName(),
        faker.helpers.arrayElement(amountTypes),
        parseFloat(faker.commerce.price({ min: 10, max: 500, dec: 2 })),
        faker.number.int({ min: 24, max: 720 }),
        parseFloat(faker.commerce.price({ min: 1, max: 100, dec: 3 })),
        faker.helpers.arrayElement([0, 5, 10, 15, 20])
      );
    }
    const res = await client.query(
      `INSERT INTO products (title, amount_type, price_for_amount_item, valid_hours, total_amount, discount_percent) VALUES ${values.join(', ')} RETURNING id`,
      params
    );
    createdTotal += res.rows.length;
  }
  console.log(`Created ${createdTotal} products`);
  const allProducts = await getIds(client, 'products');
  return allProducts;
}

function getRandomArbitrary(min, max) {
  return Math.random() * (max - min) + min;
}

async function seedOrders({
  numOrdersPerDay = 60000,
  numDays = 60,
  delayMs = 1000,
  numPolygons = 3,
  numShops = 5,
  numClients = 15,
  numPickers = 10,
  numCouriers = 10,
  numProducts = 30,
}) {
  const client = await pool.connect();

  // console.log('delete clients...')
  // await client.query(`DELETE FROM polygons`);
  // console.log('delete clients...');
  // await client.query(`DELETE FROM clients`);
  // console.log('delete pickers...');
  // await client.query(`DELETE FROM pickers`);
  // console.log('delete couriers...');
  // await client.query(`DELETE FROM couriers`);
  // console.log('delete orders...');
  // await client.query(`DELETE FROM orders`);
  // console.log('delete products...');
  // await client.query(`DELETE FROM products`);
  // console.log('delete shops...');
  // await client.query(`DELETE FROM shops`);

  try {
    const polygonIds = await ensurePolygons(client, numPolygons);
    const shopIds = await ensureShops(client, polygonIds, numShops);
    const clientIds = await ensureClients(client, numClients);
    const pickerIds = await ensurePickers(client, shopIds, numPickers);
    const courierIds = await ensureCouriers(client, polygonIds, numCouriers);
    const productIds = await ensureProducts(client, numProducts);

    let startDate = new Date('2024-07-01T00:00:00');

    for (let dayIndex = 0; dayIndex < numDays; dayIndex++) {
      const dayStart = new Date(startDate.getTime() + dayIndex * 86400000);
      const BATCH_SIZE = 3000;
      
      const orderGroups = {
        created: [],
        collecting: [],
        collected: [],
        delivering: [],
        delivered: [],
        completed: []
      };

      for (let i = 0; i < numOrdersPerDay; i++) {
        const status = faker.helpers.arrayElement(ORDER_STATUS);
        const shopId = faker.helpers.arrayElement(shopIds);
        const clientId = faker.helpers.arrayElement(clientIds);
        const pickerId = pickerIds.length > 0 ? faker.helpers.arrayElement(pickerIds) : null;
        const courierId = courierIds.length > 0 ? faker.helpers.arrayElement(courierIds) : null;
        const payment = faker.datatype.boolean();
        const city = randomCity();

        const offsetInSeconds = Math.floor(i * (86400 / numOrdersPerDay));
        const createDate = new Date(dayStart.getTime() + offsetInSeconds * 1000);

        const collectingStart = new Date(createDate.getTime() + 86400000);
        const collectingEnd = new Date(collectingStart.getTime() + getRandomArbitrary(1, 24) * 60 * 60 * 1000);
        const courierAssigned = new Date(collectingEnd.getTime() + 86400000);
        const courierTake = new Date(courierAssigned.getTime() + 86400000);
        const courierDelivered = new Date(courierTake.getTime() + getRandomArbitrary(1, 24) * 60 * 60 * 1000);
        const completed = new Date(courierDelivered.getTime() + 86400000);

        if (status === 'created') {
          orderGroups.created.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, null]);
        } else if (status === 'collecting') {
          const payDate = payment && faker.datatype.boolean() ? new Date(createDate.getTime() + 43200) : null;
          orderGroups.collecting.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, collectingStart, payDate]);
        } else if (status === 'collected') {
          const payDate = payment && faker.datatype.boolean() ? new Date(collectingStart.getTime() + 43200) : null;
          orderGroups.collected.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, collectingStart, collectingEnd, payDate]);
        } else if (status === 'delivering') {
          const payDate = payment && faker.datatype.boolean() ? new Date(collectingEnd.getTime() + 43200) : null;
          orderGroups.delivering.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, collectingStart, collectingEnd, courierAssigned, courierTake, payDate]);
        } else if (status === 'delivered') {
          const payDate = payment && faker.datatype.boolean() ? new Date(courierTake.getTime() + 43200) : null;
          orderGroups.delivered.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, collectingStart, collectingEnd, courierAssigned, courierTake, courierDelivered, payDate]);
        } else {
          const payDate = payment ? new Date(courierDelivered.getTime() + 43200) : null;
          orderGroups.completed.push([city, status, shopId, clientId, pickerId, courierId, payment, createDate, collectingStart, collectingEnd, courierAssigned, courierTake, courierDelivered, completed, payDate]);
        }
      }

      const insertBatch = async (orders, columns) => {
        if (orders.length === 0) return;
        for (let i = 0; i < orders.length; i += BATCH_SIZE) {
          const batch = orders.slice(i, i + BATCH_SIZE);
          const values = [];
          const params = [];
          
          let paramIndex = 1;
          for (const order of batch) {
            const placeholders = order.map(() => `$${paramIndex++}`);
            values.push(`(${placeholders.join(', ')})`);
          }
          
          for (const order of batch) {
            params.push(...order);
          }

          // console.log(columns, values, params, params.length, values.length);
          
          await client.query(
            `INSERT INTO orders (${columns.join(', ')}) VALUES ${values.join(', ')}`,
            params
          );
        }
      };

      await insertBatch(orderGroups.created, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'pay_date']);
      await insertBatch(orderGroups.collecting, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'collecting_start_date', 'pay_date']);
      await insertBatch(orderGroups.collected, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'collecting_start_date', 'collecting_end_date', 'pay_date']);
      await insertBatch(orderGroups.delivering, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'collecting_start_date', 'collecting_end_date', 'courier_assigned_date', 'courier_take_date', 'pay_date']);
      await insertBatch(orderGroups.delivered, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'collecting_start_date', 'collecting_end_date', 'courier_assigned_date', 'courier_take_date', 'courier_delivered_date', 'pay_date']);
      await insertBatch(orderGroups.completed, ['city', 'status', 'shop_id', 'client_id', 'picker_id', 'courier_id', 'payment', 'create_date', 'collecting_start_date', 'collecting_end_date', 'courier_assigned_date', 'courier_take_date', 'courier_delivered_date', 'completed_date', 'pay_date']);
      
      console.log(`Day ${dayIndex + 1}/${numDays} created (${numOrdersPerDay} orders, start: ${dayStart.toISOString()})`);

      // if (dayIndex < numDays - 1) {
      //   await new Promise((resolve) => setTimeout(resolve, delayMs));
      // }
    }

    console.log('Orders seeding completed!');
  } catch (err) {
    console.error('Error seeding orders:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

seedOrders({
  numOrdersPerDay: 100000,
  numDays: 365,
  delayMs: 2000,
  numPolygons: 100_000,
  numShops: 100_000,
  numClients: 100_000,
  numPickers: 100_000,
  numCouriers: 100_000,
  numProducts: 300_000,
});
