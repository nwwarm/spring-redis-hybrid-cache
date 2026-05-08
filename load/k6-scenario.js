// k6 load scenario for the hybrid-cache soak smoke app.
//
// Run locally:
//     k6 run load/k6-scenario.js
//
// Requires the soak app on localhost:8080 (see soak/README.md).
// Per-tier numbers ride to DESIGN.md §11 / Production baselines on
// each release. Do NOT inline numbers in this file — keep the
// scenario shape stable and the published table the single source.

import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        steady: {
            executor: 'constant-arrival-rate',
            rate: 1000,
            timeUnit: '1s',
            duration: '5m',
            preAllocatedVUs: 100,
            maxVUs: 500,
        },
    },
    thresholds: {
        // Sanity checks; per-tier published baselines live in
        // DESIGN.md §11.
        'http_req_failed{tier:near}': ['rate<0.01'],
        'http_req_failed{tier:async}': ['rate<0.01'],
        'http_req_failed{tier:dist}': ['rate<0.01'],
        'http_req_duration{tier:near}': ['p(99)<50'],
        'http_req_duration{tier:dist}': ['p(99)<100'],
    },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
    const op = Math.random() * 100;
    if (op < 50) {
        const id = Math.floor(Math.random() * 10000);
        const r = http.get(`${BASE}/cache/products/${id}`,
            { tags: { tier: 'near' } });
        check(r, { 'near 2xx': res => res.status >= 200 && res.status < 300 });
    } else if (op < 80) {
        const id = Math.floor(Math.random() * 10000);
        const r = http.get(`${BASE}/cache/users/${id}`,
            { tags: { tier: 'async' } });
        check(r, { 'async 2xx': res => res.status >= 200 && res.status < 300 });
    } else if (op < 95) {
        const id = Math.floor(Math.random() * 1000);
        const r = http.get(`${BASE}/cache/sessions/tok-${id}`,
            { tags: { tier: 'dist' } });
        check(r, { 'dist 2xx': res => res.status >= 200 && res.status < 300 });
    } else {
        http.del(`${BASE}/cache/products`, null, { tags: { tier: 'evict' } });
    }
}
