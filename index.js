// listen to the grafana alerts and call the azure function
const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const request = require('request');
const http = require('http')


app.listen(3000, () => {
  console.log('Server listening on port 3000');
});