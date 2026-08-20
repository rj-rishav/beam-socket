// A plain `ws` client for examples/cluster — connect to any one node, send a
// message, and watch it arrive at clients connected to the OTHER two nodes
// (see README.md for the full 3-terminal walkthrough).
//
//   node examples/cluster/client.mjs 9101 "hello from node 1's client"
import { WebSocket } from 'ws';

const port = Number(process.argv[2] ?? 9101);
const message = process.argv[3];

const ws = new WebSocket(`ws://127.0.0.1:${port}`);

ws.on('open', () => {
  console.log(`connected to ws://127.0.0.1:${port}`);
  if (message) {
    ws.send(message);
    console.log(`sent: ${message}`);
  }
});

ws.on('message', (data) => {
  console.log(`received: ${data.toString('utf8')}`);
});

ws.on('close', (code, reason) => {
  console.log(`closed (${code}) ${reason}`);
});

process.on('SIGINT', () => {
  ws.close();
  process.exit(0);
});
