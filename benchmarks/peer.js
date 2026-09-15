// The process under measurement: it takes what the driver sends it, and reports what that cost.
//
// The socket's write methods and `Buffer.concat` are counted here rather than in the library.
// What a message costs a process on this path is mostly the syscalls it makes and the bytes it
// copies on the way in: both are counted per message, beside the CPU the process spent.

const net = require('node:net');
const { PerformanceObserver } = require('node:perf_hooks');
const amqp = require('../channel_api.js');

const counters = { write: 0, writev: 0, chunks: 0, concats: 0, copies: 0, copied: 0, collections: 0, gc: 0 };

const write = net.Socket.prototype._write;
const writev = net.Socket.prototype._writev;
const concat = Buffer.concat;
const copy = Buffer.prototype.copy;

net.Socket.prototype._write = function (...args) {
  counters.write++;
  counters.chunks++;

  return write.apply(this, args);
};

net.Socket.prototype._writev = function (chunks, ...rest) {
  counters.writev++;
  counters.chunks += chunks.length;

  return writev.apply(this, [chunks, ...rest]);
};

// Every byte moved from one buffer to another is counted, however it is moved: concatenating a
// list and copying into a buffer are the same cost, and a change that replaces one with the other
// has to show up as the difference it makes rather than as a metric it steps around.
Buffer.concat = (list, length) => {
  counters.concats++;
  counters.copied += length ?? list.reduce((sum, buffer) => sum + buffer.length, 0);

  return concat(list, length);
};

Buffer.prototype.copy = function (target, start, from, to) {
  const copied = copy.call(this, target, start, from, to);

  counters.copies++;
  counters.copied += copied;

  return copied;
};

new PerformanceObserver((list) => {
  for (const entry of list.getEntries()) {
    counters.collections++;
    counters.gc += entry.duration;
  }
}).observe({ entryTypes: ['gc'] });

const QUEUE = 'amqplib.benchmark';

const main = async () => {
  const url = process.env.AMQPLIB_BENCHMARK_URL ?? 'amqp://localhost';
  const mode = process.env.AMQPLIB_BENCHMARK_MODE ?? 'deliver';

  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE, { durable: false, autoDelete: true });
  await channel.purgeQueue(QUEUE);
  await channel.prefetch(0);

  let messages = 0;
  let bytes = 0;

  // `deliver` reads what arrives and nothing else, so that what it costs is what taking a message
  // in costs; `turn` answers each one and acknowledges it, which is the two frames a served
  // request leaves in.
  const consumer =
    mode === 'deliver'
      ? (message) => {
          messages++;
          bytes += message.content.length;
        }
      : (message) => {
          messages++;
          bytes += message.content.length;
          channel.sendToQueue(message.properties.replyTo, message.content, {
            correlationId: message.properties.correlationId,
          });
          channel.ack(message);
        };

  await channel.consume(QUEUE, consumer, { noAck: mode === 'deliver' });

  process.on('message', (ask) => {
    // ends by returning rather than by a signal, so that what a run records on its way out — a
    // profile, a coverage report — is written
    if (ask === 'stop') process.exit(0);

    const { minorPageFault, majorPageFault, voluntaryContextSwitches } = process.resourceUsage();

    process.send({
      messages,
      bytes,
      cpu: process.cpuUsage(),
      faults: minorPageFault + majorPageFault,
      switches: voluntaryContextSwitches,
      ...counters,
    });
  });

  process.send('ready');
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
