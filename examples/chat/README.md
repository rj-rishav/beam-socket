# BeamSocket chat example

A minimal real-time chat — a room, a broadcast, and a browser client. Every
message fans out to the whole room in Rust; the JS handler just forwards it.

## Run

```bash
node examples/chat/server.mjs        # ws://localhost:8080
```

Then open `examples/chat/index.html` in two or three browser tabs and type.
Messages broadcast to every connected tab; join/leave notices show the live
connection count from `io.connectionCount()`.

## The whole server

```ts
import { BeamSocket } from 'beamsocket';

const io = new BeamSocket({});
io.on('connection', (socket) => {
  socket.join('chat');
  socket.on('message', (data) => io.toRoom('chat').send(data)); // fan-out in Rust
});
await io.listen(8080);
```

That's it. Rooms, fan-out, and the connection lifecycle are handled by the Rust
engine; your code stays in JavaScript.
