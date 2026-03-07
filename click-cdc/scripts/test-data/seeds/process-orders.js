// process-orders.js
import pg from 'pg';
import 'dotenv/config';

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function getIds(client, tableName) {
  const res = await client.query(`SELECT id FROM ${tableName}`);
  return res.rows.map(row => row.id);
}

async function getAvailablePicker(client, shopId) {
  const res = await client.query(
    `SELECT id FROM pickers WHERE shop_id = $1 AND (status = 'free' OR $1 IS NULL) LIMIT 1`,
    [shopId]
  );
  return res.rows.length > 0 ? res.rows[0].id : null;
}

async function getAvailableCourier(client, polygonId) {
  const res = await client.query(
    `SELECT id FROM couriers WHERE polygon_id = $1 AND (status = 'on_work' OR $1 IS NULL) LIMIT 1`,
    [polygonId]
  );
  return res.rows.length > 0 ? res.rows[0].id : null;
}

async function processOrders({
  batchSize = 5,
  intervalMs = 2000,
}) {
  const client = await pool.connect();

  try {
    while (true) {
      const orders = await client.query(
        `SELECT o.id, o.status, o.shop_id, s.polygon_id
         FROM orders o
         JOIN shops s ON o.shop_id = s.id
         WHERE o.status IN ('created', 'collecting', 'collected', 'delivering')
         LIMIT $1`,
        [batchSize]
      );

      if (orders.rows.length === 0) {
        console.log('No orders to process');
        await new Promise(resolve => setTimeout(resolve, intervalMs));
        continue;
      }

      for (const order of orders.rows) {
        const now = new Date();

        if (order.status === 'created') {
          const pickerId = await getAvailablePicker(client, order.shop_id);
          if (pickerId) {
            await client.query(
              `UPDATE orders SET status = 'collecting', picker_id = $1, collecting_start_date = $2 WHERE id = $3`,
              [pickerId, now, order.id]
            );
            await client.query(`UPDATE pickers SET status = 'busy' WHERE id = $1`, [pickerId]);
            console.log(`Order ${order.id}: created -> collecting (picker: ${pickerId})`);
          } else {
            console.log(`Order ${order.id}: no picker available`);
          }
        } else if (order.status === 'collecting') {
          await client.query(
            `SELECT picker_id FROM orders WHERE id = $1`,
            [order.id]
          ).then(res => {
            if (res.rows.length > 0 && res.rows[0].picker_id) {
              client.query(`UPDATE pickers SET status = 'free' WHERE id = $1`, [res.rows[0].picker_id]);
            }
          });
          
          await client.query(
            `UPDATE orders SET status = 'collected', collecting_end_date = $1 WHERE id = $2`,
            [now, order.id]
          );
          console.log(`Order ${order.id}: collecting -> collected`);
        } else if (order.status === 'collected') {
          const courierId = await getAvailableCourier(client, order.polygon_id);
          if (courierId) {
            await client.query(
              `UPDATE orders SET status = 'delivering', courier_id = $1, courier_assigned_date = $2, courier_take_date = $3 WHERE id = $4`,
              [courierId, now, now, order.id]
            );
            console.log(`Order ${order.id}: collected -> delivering (courier: ${courierId})`);
          } else {
            console.log(`Order ${order.id}: no courier available`);
          }
        } else if (order.status === 'delivering') {
          await client.query(
            `UPDATE orders SET status = 'delivered', courier_delivered_date = $1 WHERE id = $2`,
            [now, order.id]
          );
          console.log(`Order ${order.id}: delivering -> delivered`);
        }
      }

      await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
  } catch (err) {
    console.error('Error processing orders:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

processOrders({
  batchSize: 5,
  intervalMs: 2000,
});
