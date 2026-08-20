// BeamSocket chat — the whole server in ~15 lines. Every message fans out to
// the room entirely in Rust; the JS handler just forwards the payload.
//
//   node examples/chat/server.mjs      # ws://localhost:8080
//   then open examples/chat/index.html in a couple of browser tabs
//
// In your own project this import is just:  import { BeamSocket } from 'beamsocket';
import { BeamSocket } from '../../packages/beamsocket/dist/index.js';

const io = new BeamSocket({});

io.on('connection', (socket) => {
  socket.join('chat');
  io.toRoom('chat').send(JSON.stringify({ sys: `someone joined · ${io.connectionCount()} online` }));

  socket.on('message', (data) => {
    // One FFI call; the fan-out to every room member happens in Rust.
    io.toRoom('chat').send(data);
  });

  socket.on('close', () => {
    io.toRoom('chat').send(JSON.stringify({ sys: `someone left · ${io.connectionCount()} online` }));
  });
});

const port = await io.listen(8080);
console.log(`BeamSocket chat on ws://localhost:${port}`);
console.log('Open examples/chat/index.html in two browser tabs to try it.');
