# Benchmarks

What a message costs the process that takes it in: the CPU it spends, the bytes it copies on the
way, the page faults and socket writes it makes.

```shell
$ npm run rabbitmq-4
$ npm run benchmark
$ npm run benchmark -- deliver.448k,turn.1k     # a selection
```

The consuming process is started on its own — that is where its CPU is read from — and the driver
sends at a fixed rate without waiting for anything, which is how traffic arrives. A rate below what
the process can take is the honest place to read latency.

## What it reports

| column | what it is |
| --- | --- |
| `messages/s` | taken in during the window, over its length |
| `CPU µs` | user and system time of that process, per message |
| `system µs` | how much of it was the kernel's |
| `faults` | page faults it took, per message |
| `RSS MB` | what it was resident in memory while the window ran, on average |
| `peak MB` | the most it was resident during the window |
| `buffers MB` | of that, what the bytes behind its Buffers came to, on average |
| `copied KB` | bytes moved from one buffer to another, per message |
| `copies` | how many moves that took |
| `writes`, `writev` | socket write calls, per message |
| `GC µs` | what garbage collection cost in the window, per message |
| `GC/1k` | collections in the window, per thousand messages |
| `p50`, `p99` | what the driver waited for a reply, milliseconds, where there is one |

`copied` counts every `Buffer.concat` and every `Buffer.copy` the process makes, so that a change
which replaces one with the other shows up as the difference it makes rather than as a metric it
steps around. `writes` and `writev` are the mechanism rather than the outcome: a frame written as
its own syscall costs more than one written together with its neighbours.

## The scenarios

`deliver.*` take a message in and do nothing else, so what is read is what taking it in costs.
`turn.*` answer each message and acknowledge it, which is the two frames a served request leaves in.
The sizes either side of 64 KB are there because a socket reads 64 KB at a time and a body frame
may be up to `frameMax`: which of those two is larger decides whether a message arrives whole or in
pieces.

## Reading a result

A number here means something only beside another number from the same machine. Run it on the
revision you are changing, then on your change, and compare the columns; both rounds are printed
rather than averaged, so a run that drifted is visible as a run that drifted.

**The allocator is part of what is measured.** A process that allocates and frees a buffer of tens
of kilobytes per message hands those pages back to the system as often as glibc's trim threshold
tells it to, and that shows up as `system µs` and `faults` rather than as anything in the library.
`MALLOC_TRIM_THRESHOLD_` in the environment is what separates the two:

```shell
$ MALLOC_TRIM_THRESHOLD_=67108864 npm run benchmark -- deliver.64k
```

## Without a broker

```shell
$ node benchmarks/parse.js --size=458752 --reads=65536 --frame=131072 --messages=4000
```

The same bytes, in the same pieces, through the same loop, with nothing else in the process: what
taking frames off the wire costs on its own. `--reads` is what a socket hands over at a time, and
`--frame` what the server was told it may send.
