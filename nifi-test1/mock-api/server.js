const express = require('express');
const app = express();
const port = 9000;

// API 1: trả số lượng cột của bảng A
app.get('/count/tableA', (req, res) => {
  res.json({
    table: "A",
    columns: 5,
    description: "DEV mock API 1"
  });
});

// API 2: trả số lượng cột của bảng B
app.get('/count/tableB', (req, res) => {
  res.json({
    table: "B",
    columns: 12,
    extra: "some extra info",
    description: "DEV mock API 2"
  });
});

app.listen(port, () => {
  console.log(`Mock API server chạy tại http://localhost:${port}`);
});

