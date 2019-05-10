#!/usr/bin/env node
process.env.UV_THREADPOOL_SIZE = 128;

const Mercury = require('@postlight/mercury-parser');

url = process.argv[2];
Mercury.parse(url).then(result => console.log(result));
