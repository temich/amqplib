const { describe, it } = require('node:test');
const assert = require('node:assert');
const connection = require('../lib/connection');
const Frames = connection.Connection;
const HEARTBEAT = require('../lib/frame').HEARTBEAT;
const Stream = require('node:stream');
const PassThrough = Stream.PassThrough;
const { choice, forAll, repeat, label, sequence, transform, sized } = require('claire');
const amqp = require('./lib/data');
const assertEqualModuloDefaults = require('./lib/util').assertEqualModuloDefaults;
const defs = require('../lib/defs');

// We'll need to supply a stream which we manipulate ourselves
function inputs() {
  // don't coalesce buffers, since that could mess up properties (e.g., encoded frame size)
  return new PassThrough({ objectMode: true });
}

const HB = Buffer.from([
  defs.constants.FRAME_HEARTBEAT,
  0,
  0, // channel 0
  0,
  0,
  0,
  0, // zero size
  defs.constants.FRAME_END,
]);

describe('Frame', () => {

  describe('Explicit parsing', () => {
    it('Parse heartbeat', () => {
      const input = inputs();
      const frames = new Frames(input);
      input.write(HB);
      assert.ok(frames.recvFrame() === HEARTBEAT);
      assert.ok(!frames.recvFrame());
    });

    it('Parse partitioned', () => {
      const input = inputs();
      const frames = new Frames(input);
      input.write(HB.subarray(0, 3));
      assert.ok(!frames.recvFrame());
      input.write(HB.subarray(3));
      assert.ok(frames.recvFrame() === HEARTBEAT);
      assert.ok(!frames.recvFrame());
    });

    function testBogusFrame(name, bytes) {
      it(name, (_t, done) => {
        const input = inputs();
        const frames = new Frames(input);
        frames.frameMax = 5; //for the max frame test
        input.write(Buffer.from(bytes));
        frames.step((err, _frame) => {
          if (err != null) done();
          else assert.fail('Was a bogus frame!');
        });
      });
    }

    testBogusFrame('Wrong sized frame', [
      defs.constants.FRAME_BODY,
      0,
      0,
      0,
      0,
      0,
      0, // zero length
      65, // but a byte!
      defs.constants.FRAME_END,
    ]);

    testBogusFrame('Unknown method frame', [
      defs.constants.FRAME_METHOD,
      0,
      0,
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      0, // garbage ID
      defs.constants.FRAME_END,
    ]);
  });

  describe('Gathering a frame from chunks', () => {
    function bodyFrame(channel, payload) {
      const header = Buffer.alloc(7);
      header.writeUInt8(defs.constants.FRAME_BODY, 0);
      header.writeUInt16BE(channel, 1);
      header.writeUInt32BE(payload.length, 3);
      return Buffer.concat([header, payload, Buffer.from([defs.constants.FRAME_END])]);
    }

    // what `recvFrame` returns for each chunk as it arrives, the way a socket's `readable` drives it
    function receive(chunks) {
      const input = inputs();
      const frames = new Frames(input);
      const received = [];
      for (const chunk of chunks) {
        input.write(chunk);
        let frame = frames.recvFrame();
        while (frame) {
          received.push(frame);
          frame = frames.recvFrame();
        }
      }
      return received;
    }

    function split(bytes, size) {
      const chunks = [];
      for (let offset = 0; offset < bytes.length; offset += size) chunks.push(bytes.subarray(offset, offset + size));
      return chunks;
    }

    it('copies the bytes of a frame about once, however many chunks it arrives in', () => {
      const frame = bodyFrame(1, Buffer.alloc(128 * 1024, 0x61));
      const concat = Buffer.concat;
      let copied = 0;
      Buffer.concat = (list, length) => {
        const joined = concat(list, length);
        copied += joined.length;
        return joined;
      };
      let received;
      try {
        received = receive(split(frame, 2048));
      } finally {
        Buffer.concat = concat;
      }
      assert.strictEqual(received.length, 1);
      assert.ok(copied <= 2 * frame.length, `copied ${copied} bytes to gather a frame of ${frame.length}`);
    });

    it('reads the same frames wherever the chunks are cut', () => {
      const small = Buffer.from('small');
      const large = Buffer.alloc(10000);
      for (let i = 0; i < large.length; i++) large[i] = i % 251;
      const stream = Buffer.concat([HB, bodyFrame(1, small), bodyFrame(2, large), HB, bodyFrame(3, Buffer.alloc(0))]);
      for (const size of [1, 2, 6, 7, 8, 9, 13, 100, 4096, stream.length - 1, stream.length]) {
        const received = receive(split(stream, size));
        assert.strictEqual(received.length, 5, `cut every ${size}`);
        assert.strictEqual(received[0], HEARTBEAT);
        assert.deepStrictEqual([received[1].channel, received[1].content], [1, small]);
        assert.deepStrictEqual([received[2].channel, received[2].content], [2, large]);
        assert.strictEqual(received[3], HEARTBEAT);
        assert.deepStrictEqual([received[4].channel, received[4].content], [3, Buffer.alloc(0)]);
      }
    });
  });

  const Trace = label('frame trace', repeat(choice.apply(choice, amqp.methods)));

  describe('Parsing', () => {
    function testPartitioning(partition) {
      return forAll(Trace)
        .satisfy((t) => {
          const bufs = [];
          const input = inputs();
          const frames = new Frames(input);
          let i = 0;
          let ex;
          frames.accept = (f) => {
            // A minor hack to make sure we get the assertion exception;
            // otherwise, it's just a test that we reached the line
            // incrementing `i` for each frame.
            try {
              assertEqualModuloDefaults(t[i], f.fields);
            } catch (e) {
              ex = e;
            }
            i++;
          };

          t.forEach((f) => {
            f.channel = 0;
            bufs.push(defs.encodeMethod(f.id, 0, f.fields));
          });

          partition(bufs).forEach((chunk) => input.write(chunk));
          frames.acceptLoop();
          if (ex) throw ex;
          return i === t.length;
        })
        .asTest({ times: 20 });
    }

    it('Parse trace of methods', testPartitioning((bufs) => bufs));

    it("Parse concat'd methods", testPartitioning((bufs) => [Buffer.concat(bufs)]));

    it('Parse partitioned methods', testPartitioning((bufs) => {
      const full = Buffer.concat(bufs);
      const onethird = Math.floor(full.length / 3);
      const twothirds = 2 * onethird;
      return [full.subarray(0, onethird), full.subarray(onethird, twothirds), full.subarray(twothirds)];
    }));
  });

  const FRAME_MAX_MAX = 4096 * 4;
  const FRAME_MAX_MIN = 4096;
  const FrameMax = amqp.rangeInt('frame max', FRAME_MAX_MIN, FRAME_MAX_MAX);
  const Body = sized((_n) => Math.floor(Math.random() * FRAME_MAX_MAX), repeat(amqp.Octet));

  const Content = transform(
    (args) => ({
      method: args[0].fields,
      header: args[1].fields,
      body: Buffer.from(args[2]),
    }),
    sequence(amqp.methods['BasicDeliver'], amqp.properties['BasicProperties'], Body),
  );

  describe('Content framing', () => {
    it('Adhere to frame max', forAll(Content, FrameMax)
      .satisfy((content, max) => {
        const input = inputs();
        const frames = new Frames(input);
        frames.frameMax = max;
        frames.sendMessage(0, defs.BasicDeliver, content.method, defs.BasicProperties, content.header, content.body);
        let _i = 0;
        let largest = 0;
        let frame = input.read();
        while (frame) {
          _i++;
          if (frame.length > largest) largest = frame.length;
          if (frame.length > max) {
            return false;
          }
          frame = input.read();
        }
        // The ratio of frames to 'contents' should always be >= 2
        // (one properties frame and at least one content frame); > 2
        // indicates fragmentation. The largest is always, of course <= frame max
        //console.log('Frames: %d; frames per message: %d; largest frame %d', _i, _i / t.length, largest);
        return true;
      })
      .asTest());
  });
});
