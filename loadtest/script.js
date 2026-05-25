import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const deliveryLatency = new Trend('delivery_latency');

const TARGET_HOST = __ENV.TARGET_HOST || 'localhost:8080';
const BASE_URL = `http://${TARGET_HOST}`;

export const options = {
  stages: [
    { duration: '30s', target: 50 },
    { duration: '1m',  target: 200 },
    { duration: '30s', target: 500 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    errors: ['rate<0.01'],
  },
};

const channels = ['sms', 'email', 'push'];
const priorities = ['high', 'normal', 'low'];

function randomChannel() {
  return channels[Math.floor(Math.random() * channels.length)];
}

function randomPriority() {
  return priorities[Math.floor(Math.random() * priorities.length)];
}

function recipientFor(channel) {
  if (channel === 'email') return `user${Math.floor(Math.random() * 10000)}@example.com`;
  if (channel === 'sms')   return `+9055512${String(Math.floor(Math.random() * 100000)).padStart(5, '0')}`;
  return `device-token-${Math.floor(Math.random() * 100000)}`;
}

export default function () {
  const channel = randomChannel();
  const payload = JSON.stringify({
    recipient: recipientFor(channel),
    channel: channel,
    content: `Test notification at ${new Date().toISOString()}`,
    priority: randomPriority(),
  });

  const params = { headers: { 'Content-Type': 'application/json' } };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/notifications`, payload, params);
  deliveryLatency.add(Date.now() - start);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'has id': (r) => {
      try { return JSON.parse(r.body).data?.id !== undefined; } catch { return false; }
    },
  });

  errorRate.add(!ok);
  sleep(0.01);
}

export function teardown() {
  const res = http.get(`${BASE_URL}/metrics`);
  console.log('Final metrics:', res.body);
}
