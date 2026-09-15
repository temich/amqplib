// Sends what a scenario calls for and reports what taking it in cost the peer, per message.

const { fork } = require('node:child_process');
const { join } = require('node:path');
const amqp = require('../channel_api.js');

const QUEUE = 'amqplib.benchmark';
const WINDOW = 5000;
const WARMUP = 2000;

const SCENARIOS = [
  { id: 'deliver.448k', mode: 'deliver', size: 448 * 1024, rate: 100 },
  { id: 'deliver.64k', mode: 'deliver', size: 64 * 1024, rate: 700 },
  { id: 'deliver.1k', mode: 'deliver', size: 1024, rate: 20000 },
  { id: 'turn.1k', mode: 'turn', size: 1024, rate: 5000 },
  { id: 'turn.448k', mode: 'turn', size: 448 * 1024, rate: 50 },
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Starts the peer and waits for it to say it is consuming. */
const start = async (url, mode) => {
  const peer = fork(join(__dirname, 'peer.js'), {
    env: { ...process.env, AMQPLIB_BENCHMARK_URL: url, AMQPLIB_BENCHMARK_MODE: mode },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });

  await new Promise((resolve, reject) => {
    peer.once('message', resolve);
    peer.once('exit', (code) => reject(new Error(`the peer exited with ${code}`)));
  });

  return peer;
};

/** What the peer has spent so far. */
/** Ends the peer and waits for it to go. */
const stop = async (peer) => {
  const ended = new Promise((resolve) => peer.once('exit', resolve));

  peer.send('stop');

  const abandoned = setTimeout(() => peer.kill(), 2000);

  await ended;
  clearTimeout(abandoned);
};

const meter = (peer) =>
  new Promise((resolve) => {
    peer.once('message', resolve);
    peer.send('stats');
  });

/**
 * Sends at the rate asked for without waiting for anything: what is measured is what taking the
 * messages in costs, and a sender that waits would measure the round trip instead.
 */
const send = (channel, scenario, replyTo, deadline, sent) => {
  const payload = Buffer.alloc(scenario.size, 'x');
  const started = Date.now();
  let count = 0;

  return new Promise((resolve) => {
    const tick = () => {
      if (Date.now() >= deadline) {
        clearInterval(timer);
        resolve(count);

        return;
      }

      const due = Math.round(((Date.now() - started) / 1000) * scenario.rate);

      while (count < due) {
        count++;
        sent.push(process.hrtime.bigint());
        channel.sendToQueue(QUEUE, payload, replyTo === undefined ? {} : { replyTo, correlationId: String(count) });
      }
    };

    const timer = setInterval(tick, 2);
  });
};

const percentile = (values, share) => {
  if (values.length === 0) return Number.NaN;

  const sorted = [...values].sort((a, b) => a - b);

  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * share))];
};

const round = async (url, scenario) => {
  const peer = await start(url, scenario.mode);
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();

  const latencies = [];
  const sent = [];
  let replyTo;

  if (scenario.mode === 'turn') {
    const replies = await channel.assertQueue('', { exclusive: true });

    replyTo = replies.queue;

    await channel.consume(
      replyTo,
      (message) => {
        const at = sent[Number(message.properties.correlationId) - 1];

        if (at !== undefined) latencies.push(Number(process.hrtime.bigint() - at) / 1e6);
      },
      { noAck: true },
    );
  }

  try {
    await send(channel, scenario, replyTo, Date.now() + WARMUP, []);
    await sleep(200);

    const before = await meter(peer);

    latencies.length = 0;

    const published = await send(channel, scenario, replyTo, Date.now() + WINDOW, sent);

    await sleep(500);

    const after = await meter(peer);
    const messages = after.messages - before.messages;
    const user = after.cpu.user - before.cpu.user;
    const system = after.cpu.system - before.cpu.system;
    const delta = (name) => after[name] - before[name];

    return {
      published,
      messages,
      rate: Math.round((messages / WINDOW) * 1000),
      cpu: (user + system) / messages,
      system: system / messages,
      copied: delta('copied') / messages,
      concats: (delta('concats') + delta('copies')) / messages,
      write: delta('write') / messages,
      writev: delta('writev') / messages,
      gc: delta('gc') / messages,
      collections: delta('collections') / messages,
      p50: percentile(latencies, 0.5),
      p99: percentile(latencies, 0.99),
    };
  } finally {
    await channel.close().catch(() => undefined);
    await connection.close().catch(() => undefined);

    await stop(peer);
  }
};

const table = (results) => {
  const rows = [
    '| scenario | messages/s | CPU µs | system µs | copied KB | copies | writes | writev | GC µs | p50 ms | p99 ms |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
  ];

  for (const [scenario, result] of results) {
    const latency = Number.isNaN(result.p50) ? ['—', '—'] : [result.p50.toFixed(2), result.p99.toFixed(2)];

    rows.push(
      `| ${scenario.id} | ${result.rate} | ${result.cpu.toFixed(1)} | ${result.system.toFixed(1)} | ${(result.copied / 1024).toFixed(1)} | ` +
        `${result.concats.toFixed(2)} | ${result.write.toFixed(2)} | ${result.writev.toFixed(2)} | ` +
        `${result.gc.toFixed(1)} | ${latency[0]} | ${latency[1]} |`,
    );
  }

  return rows.join('\n');
};

const main = async () => {
  const url = process.env.AMQPLIB_BENCHMARK_URL ?? 'amqp://localhost';
  const selected = process.argv[2] === undefined ? SCENARIOS : SCENARIOS.filter((one) => process.argv[2].split(',').includes(one.id));

  for (const pass of [1, 2]) {
    const results = [];

    for (const scenario of selected) {
      process.stderr.write(`round ${pass}: ${scenario.id}\n`);
      results.push([scenario, await round(url, scenario)]);
    }

    console.log(`\n## Round ${pass}\n`);
    console.log(table(results));
  }
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
