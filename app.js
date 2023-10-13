const express = require("express");
const app = express();
const cors = require("cors");
const sqlite3 = require("sqlite3");

app.use(cors());
app.use(express.json());

const db = new sqlite3.Database("./zenith.sqlite3", (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log("Connected to the zenith database.");
});

app.get("/", (req, res) => {
  res.send("Hello World");
});

app.get("/price", async (req, res) => {
  const frame = req.query.frame;
  const startTime = (
    Math.floor(req.query.startTime / frame) * frame
  ).toString();
  const endTime = (Math.ceil(req.query.endTime / frame) * frame).toString();

  let bucketListKeys = new Set();
  let bucketList = {};

  let candles = [];

  const sql = `SELECT * FROM marked_price WHERE strftime('%s', timestamp) BETWEEN ? AND ?`;
  db.all(sql, [startTime, endTime], (err, rows) => {
    if (err) {
      throw err;
    }
    console.log(rows.length);

    if (!rows.length) {
      res.json([]);
      return;
    }

    for (let i = 0; i < rows.length; i++) {
      let row = rows[i];
      let unixTimestamp = new Date(row.timestamp).getTime();
      let bucketIndex = Math.floor(unixTimestamp / (frame * 1000));
      bucketListKeys.add(bucketIndex);
      if (!bucketList[bucketIndex]) {
        bucketList[bucketIndex] = [];
      }
      bucketList[bucketIndex].push(row.price);
    }
    bucketListKeys = Array.from(bucketListKeys);

    let lastTransaction = {
      price: 0,
    };

    const lastTransactionSql = `SELECT * FROM marked_price WHERE strftime('%s', timestamp) < ? ORDER BY timestamp DESC LIMIT 1`;

    db.all(lastTransactionSql, [startTime], (err, queries) => {
      if (err) {
        throw err;
      }
      if (queries.length) {
        lastTransaction = queries[0];
      }

      let candle = {};

      for (let i = 0; i < bucketListKeys.length; i++) {
        let bucket = bucketList[bucketListKeys[i]];
        if (bucket.length) {
          if (i == 0) {
            if (!queries.length) {
              candle = {
                open: bucket[0],
                high: Math.max(...bucket),
                low: Math.min(...bucket),
                close: bucket[bucket.length - 1],
                timestamp: Math.floor(
                  parseInt(bucketListKeys[i] * frame * 1000)
                ),
              };
            } else {
              candle = {
                open: lastTransaction.price,
                high: Math.max(...bucket),
                low: Math.min(...bucket),
                close: bucket[bucket.length - 1],
                timestamp: Math.floor(
                  parseInt(bucketListKeys[i] * frame * 1000)
                ),
              };
            }
          } else {
            candle = {
              open: candles[i - 1].close,
              high: Math.max(...bucket),
              low: Math.min(...bucket),
              close: bucket[bucket.length - 1],
              timestamp: Math.floor(parseInt(bucketListKeys[i] * frame * 1000)),
            };
          }
        } else {
          if (i == 0) {
            if (queries.length) {
              candle = {
                open: lastTransaction.price,
                high: lastTransaction.price,
                low: lastTransaction.price,
                close: lastTransaction.price,
                timestamp: Math.floor(
                  parseInt(bucketListKeys[i] * frame * 1000)
                ),
              };
            } else {
              continue;
            }
          } else {
            candle = {
              open: candles[i - 1].close,
              high: candles[i - 1].close,
              low: candles[i - 1].close,
              close: candles[i - 1].close,
              timestamp: Math.floor(parseInt(bucketListKeys[i] * frame * 1000)),
            };
          }
        }
        candles.push(candle);
      }
      res.json(candles);
    });
  });
});

app.listen(3001, () => {
  console.log("Server running on port 3001");
});
