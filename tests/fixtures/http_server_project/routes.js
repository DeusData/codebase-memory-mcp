const express = require('express');
const app = express();

app.post('/v1/score', (req, res) => res.json({ score: 42 }));
app.get('/v1/users/:id', (req, res) => res.json({ id: req.params.id }));
app.get('/v1/me', (req, res) => res.json({ me: true }));

module.exports = app;
