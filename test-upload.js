import fs from 'fs';
import FormData from 'form-data';
import http from 'http';

// Read test SVG
const svgContent = fs.readFileSync('test.svg');

// Create form data
const form = new FormData();
form.append('file', svgContent, 'test.svg');

// Make request
const req = http.request({
  hostname: 'localhost',
  port: 8000,
  path: '/api/docs/upload',
  method: 'POST',
  headers: {
    ...form.getHeaders(),
    'Authorization': 'Bearer test-token'
  }
});

form.pipe(req);

req.on('response', (res) => {
  console.log('Status:', res.statusCode);
  let data = '';
  res.on('data', chunk => data += chunk);
  res.on('end', () => {
    console.log('Response:', data);
  });
});

req.on('error', (err) => {
  console.error('Error:', err.message);
});
