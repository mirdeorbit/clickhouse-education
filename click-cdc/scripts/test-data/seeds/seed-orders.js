// seed-orders.js
import pg from 'pg';
import { faker } from '@faker-js/faker';
import dotenv from 'dotenv';
dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const CITIES = ['Moscow', 'Saint Petersburg', 'Kazan', 'Novosibirsk', 'Yekaterinburg'];
const ORDER_STATUS = ['created', 'collecting', 'collected', 'delivering', 'delivered', 'completed'];

const randomCity = () => faker.helpers.arrayElement(CITIES);
const randomString = (length = 8) => faker.string.alpha({ count: length, casing: 'mixed' }).toUpperCase();
const randomTimestamp = (offsetHours = 0) => {
  const date = new Date();
  date.setHours(date.getHours() + offsetHours);
  return date;
};

async function getIds(client, tableName) {
  const res = await client.query(`SELECT id FROM ${tableName}`);
  return res.rows.map(row => row.id);
}

async function ensurePolygons(client, numPolygons = 3) {
  const existing = await getIds(client, 'polygons');
  if (existing.length > 0) return existing;

  const polygonIds = [];
  for (let i = 0; i < numPolygons; i++) {
    const res = await client.query(
      `INSERT INTO polygons (name, city) VALUES ($1, $2) RETURNING id`,
      [randomString(10), randomCity()]
    );
    polygonIds.push(res.rows[0].id);
  }
  console.log(`Created ${polygonIds.length} polygons`);
  return polygonIds;
}

async function ensureShops(client, polygonIds, numShops = 5) {
  const existing = await getIds(client, 'shops');
  if (existing.length > 0) return existing;

  const shopIds = [];
  for (let i = 0; i < numShops; i++) {
    const res = await client.query(
      `INSERT INTO shops (address, city, polygon_id, start_work_time, end_work_time) 
       VALUES ($1, $2, $3, $4, $5) RETURNING id`,
      [
        faker.location.streetAddress(),
        randomCity(),
        faker.helpers.arrayElement(polygonIds),
        '08:00',
        '20:00',
      ]
    );
    shopIds.push(res.rows[0].id);
  }
  console.log(`Created ${shopIds.length} shops`);
  return shopIds;
}

async function ensureClients(client, numClients = 15) {
  const existing = await getIds(client, 'clients');
  if (existing.length > 0) return existing;

  const clientIds = [];
  for (let i = 0; i < numClients; i++) {
    const res = await client.query(
      `INSERT INTO clients (full_name, phone, address) VALUES ($1,$2,$3) RETURNING id`,
      [faker.person.fullName(), faker.phone.number({ style: 'national' }), faker.location.streetAddress()]
    );
    clientIds.push(res.rows[0].id);
  }
  console.log(`Created ${clientIds.length} clients`);
  return clientIds;
}

async function ensurePickers(client, shopIds, numPickers = 10) {
  const existing = await getIds(client, 'pickers');
  if (existing.length > 0) return existing;

  const pickerIds = [];
  const pickerStatuses = ['blocked', 'free', 'busy'];
  const networks = ['asx', 'self_delivery', 'otus_logistics'];

  for (let i = 0; i < numPickers; i++) {
    const res = await client.query(
      `INSERT INTO pickers (first_name, last_name, patronymic, phone, email, status, network, city, timezone, shop_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
      [
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
      ]
    );
    pickerIds.push(res.rows[0].id);
  }
  console.log(`Created ${pickerIds.length} pickers`);
  return pickerIds;
}

async function ensureCouriers(client, polygonIds, numCouriers = 10) {
  const existing = await getIds(client, 'couriers');
  if (existing.length > 0) return existing;

  const courierIds = [];
  const courierStatuses = ['blocked', 'on_work', 'not_on_work'];

  for (let i = 0; i < numCouriers; i++) {
    const res = await client.query(
      `INSERT INTO couriers (first_name, last_name, patronymic, phone, email, inn, city, status, company, self_employed, timezone, polygon_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
      [
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
      ]
    );
    courierIds.push(res.rows[0].id);
  }
  console.log(`Created ${courierIds.length} couriers`);
  return courierIds;
}

async function ensureProducts(client, numProducts = 30) {
  const existing = await getIds(client, 'products');
  if (existing.length > 0) return existing;

  const productIds = [];
  const amountTypes = ['weight', 'items'];

  for (let i = 0; i < numProducts; i++) {
    const res = await client.query(
      `INSERT INTO products (title, amount_type, price_for_amount_item, valid_hours, total_amount, discount_percent)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
      [
        faker.commerce.productName(),
        faker.helpers.arrayElement(amountTypes),
        parseFloat(faker.commerce.price({ min: 10, max: 500, dec: 2 })),
        faker.number.int({ min: 24, max: 720 }),
        parseFloat(faker.commerce.price({ min: 1, max: 100, dec: 3 })),
        faker.helpers.arrayElement([0, 5, 10, 15, 20])
      ]
    );
    productIds.push(res.rows[0].id);
  }
  console.log(`Created ${productIds.length} products`);
  return productIds;
}

async function seedOrders({
  numOrdersPerBatch = 5,
  numBatches = 5,
  delayMs = 1000,
  numPolygons = 3,
  numShops = 5,
  numClients = 15,
  numPickers = 10,
  numCouriers = 10,
  numProducts = 30,
}) {
  const client = await pool.connect();

  try {
    const polygonIds = await ensurePolygons(client, numPolygons);
    const shopIds = await ensureShops(client, polygonIds, numShops);
    const clientIds = await ensureClients(client, numClients);
    const pickerIds = await ensurePickers(client, shopIds, numPickers);
    const courierIds = await ensureCouriers(client, polygonIds, numCouriers);
    const productIds = await ensureProducts(client, numProducts);

    const createOrdersBatch = async (batchNum) => {
      const promises = [];
      for (let i = 0; i < numOrdersPerBatch; i++) {
        const status = faker.helpers.arrayElement(ORDER_STATUS);
        const shopId = faker.helpers.arrayElement(shopIds);
        const clientId = faker.helpers.arrayElement(clientIds);
        const pickerId = pickerIds.length > 0 ? faker.helpers.arrayElement(pickerIds) : null;
        const courierId = courierIds.length > 0 ? faker.helpers.arrayElement(courierIds) : null;
        const payment = faker.datatype.boolean();
        const city = randomCity();
        const createDate = randomTimestamp(-24);
        
        let query, params;

        if (status === 'created') {
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, null];
        } else if (status === 'collecting') {
          const payDate = payment && faker.datatype.boolean() ? randomTimestamp(-23.5) : null;
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, collecting_start_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, randomTimestamp(-23), payDate];
        } else if (status === 'collected') {
          const payDate = payment && faker.datatype.boolean() ? randomTimestamp(-22.5) : null;
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, collecting_start_date, collecting_end_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, randomTimestamp(-23), randomTimestamp(-22), payDate];
        } else if (status === 'delivering') {
          const payDate = payment && faker.datatype.boolean() ? randomTimestamp(-21.5) : null;
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, collecting_start_date, collecting_end_date, courier_assigned_date, courier_take_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, randomTimestamp(-23), randomTimestamp(-22), randomTimestamp(-21), randomTimestamp(-20), payDate];
        } else if (status === 'delivered') {
          const payDate = payment && faker.datatype.boolean() ? randomTimestamp(-19.5) : null;
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, collecting_start_date, collecting_end_date, courier_assigned_date, courier_take_date, courier_delivered_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, randomTimestamp(-23), randomTimestamp(-22), randomTimestamp(-21), randomTimestamp(-20), randomTimestamp(-19), payDate];
        } else {
          query = `INSERT INTO orders (city, status, shop_id, client_id, picker_id, courier_id, payment, create_date, collecting_start_date, collecting_end_date, courier_assigned_date, courier_take_date, courier_delivered_date, completed_date, pay_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`;
          params = [city, status, shopId, clientId, pickerId, courierId, payment, createDate, randomTimestamp(-23), randomTimestamp(-22), randomTimestamp(-21), randomTimestamp(-20), randomTimestamp(-19), randomTimestamp(-18), payment ? randomTimestamp(-18) : null];
        }

        promises.push(
          client.query(query, params)
        );
      }
      await Promise.all(promises);
      console.log(`Batch ${batchNum + 1} created`);
    };

    for (let batch = 0; batch < numBatches; batch++) {
      await createOrdersBatch(batch);
      if (batch < numBatches - 1) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
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
  numOrdersPerBatch: 5,
  numBatches: 5,
  delayMs: 2000,
  numPolygons: 3,
  numShops: 5,
  numClients: 15,
  numPickers: 10,
  numCouriers: 10,
  numProducts: 30,
});
