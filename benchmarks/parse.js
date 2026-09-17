// What taking frames off the wire costs, with no broker in it: the same bytes, in the same pieces,
// through the same loop, so that a change to the parsing is read without a network in the way.
//
// `--reads` is what a socket hands over at a time, and `--frame` what the server was told it may
// send. A body larger than a read is the case worth watching: its frames span what arrives.

const { Duplex } = require('node:stream');
const defs = require('../lib/defs');
const { makeBodyFrame } = require('../lib/frame');
const { Connection } = require('../lib/connection');

const counters = { copied: 0, copies: 0 };
const concat = Buffer.concat;
const copy = Buffer.prototype.copy;

Buffer.concat = (list, length) => {
  counters.copies++;
  counters.copied += length ?? list.reduce((sum, buffer) => sum + buffer.length, 0);

  return concat(list, length);
};

Buffer.prototype.copy = function (target, start, from, to) {
  const copied = copy.call(this, target, start, from, to);

  counters.copies++;
  counters.copied += copied;

  return copied;
};

const option = (name, fallback) => {
  const argument = process.argv.find((one) => one.startsWith(`--${name}=`));

  return argument === undefined ? fallback : Number(argument.split('=')[1]);
};

/** One delivery on the wire: the method, the properties, and the body in frames of at most `frameMax`. */
const delivery = (channel, body, frameMax) => {
  const frames = [
    defs.encodeMethod(defs.BasicDeliver, channel, {
      consumerTag: 'benchmark',
      deliveryTag: 1,
      redelivered: false,
      exchange: '',
      routingKey: 'benchmark',
    }),
    defs.encodeProperties(defs.BasicProperties, channel, body.length, { deliveryMode: 1 }),
  ];

  for (let at = 0; at < body.length; at += frameMax) frames.push(makeBodyFrame(channel, body.subarray(at, at + frameMax)));

  return Buffer.concat(frames);
};

const main = () => {
  const size = option('size', 448 * 1024);
  const reads = option('reads', 64 * 1024);
  const frameMax = option('frame', 131072) - defs.FRAME_OVERHEAD;
  const messages = option('messages', 2000);

  const wire = delivery(1, Buffer.alloc(size, 'x'), frameMax);
  const connection = new Connection(new Duplex({ read() {}, write() {} }));

  let frames = 0;

  // the accepted frames are counted and dropped: what is measured is taking them off the wire
  const drain = () => {
    while (connection.recvFrame() !== false) frames++;
  };

  // warm up the shapes the loop sees before the window is read
  for (let i = 0; i < 50; i++) {
    for (let at = 0; at < wire.length; at += reads) {
      connection.stream.push(wire.subarray(at, at + reads));
      drain();
    }
  }

  frames = 0;
  counters.copied = 0;
  counters.copies = 0;

  const cpu = process.cpuUsage();
  const started = process.hrtime.bigint();

  for (let i = 0; i < messages; i++)
    for (let at = 0; at < wire.length; at += reads) {
      connection.stream.push(wire.subarray(at, at + reads));
      drain();
    }

  const spent = process.cpuUsage(cpu);
  const elapsed = Number(process.hrtime.bigint() - started) / 1e6;
  const total = spent.user + spent.system;

  console.log(
    `| ${(size / 1024).toFixed(0)}K | ${(reads / 1024).toFixed(0)}K | ${(total / messages).toFixed(2)} | ` +
      `${(counters.copied / messages / 1024).toFixed(1)} | ${(counters.copies / messages).toFixed(2)} | ` +
      `${(frames / messages).toFixed(1)} | ${((size * messages) / 1024 / elapsed).toFixed(0)} |`,
  );
};

console.log('| message | read | CPU µs | copied KB | copies | frames | MB/s |');
console.log('| --- | --- | ---: | ---: | ---: | ---: | ---: |');
main();
