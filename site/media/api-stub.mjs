// The HTTP API order-saga and approval-gate call while make site-media records
// them (media/capture.ts). A process of its own: the capture blocks while it waits
// for a run, and a run must get its answers meanwhile. It takes every request but
// the shipment of one order, so that order's run fails at a real answer.
//
//   node media/api-stub.mjs HOST PORT REFUSED_ORDER
import { createServer } from 'node:http';

const [host, port, refused] = process.argv.slice(2);
createServer((req, res) => {
	const code = req.method !== 'PUT' ? 200 : req.url === `/orders/${refused}/shipment` ? 503 : 201;
	res.writeHead(code, { 'content-type': 'application/json' });
	res.end(JSON.stringify({ accepted: code < 400 }));
}).listen(Number(port), host, () => console.log('listening'));
