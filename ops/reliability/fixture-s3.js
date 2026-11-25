// Test-only S3 subset: no authentication or production Object Lock guarantees.
import { createServer } from 'node:http';
import { URL } from 'node:url';
import { Buffer } from 'node:buffer';
import process from 'node:process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile, readdir } from 'node:fs/promises';
const directory = '/app/fixture-data';
await mkdir(directory, { recursive: true });
createServer((request, response) => {
  void (async () => {
    const key = decodeURIComponent(new URL(request.url, 'http://fixture-s3').pathname);
    const file = `${directory}/${createHash('sha256').update(key).digest('hex')}`;
    if (key === '/health') {
      response.end('ok');
      return;
    }
    if (key === '/objects') {
      const names = (await readdir(directory)).filter((name) => name.endsWith('.json'));
      const objects = await Promise.all(
        names.map(async (name) => JSON.parse(await readFile(`${directory}/${name}`, 'utf8'))),
      );
      response.end(JSON.stringify(objects));
      return;
    }
    if (request.method === 'PUT') {
      if (
        !request.headers['x-amz-object-lock-mode'] ||
        !request.headers['x-amz-object-lock-retain-until-date']
      ) {
        response.writeHead(400);
        response.end('lock required');
        return;
      }
      const chunks = [];
      let bytes = 0;
      for await (const chunk of request) {
        bytes += chunk.length;
        if (bytes > 16 * 1024 * 1024) throw new Error('Fixture body too large');
        chunks.push(chunk);
      }
      const body = Buffer.concat(chunks);
      const version = randomUUID();
      await writeFile(file, body);
      await writeFile(
        `${file}.json`,
        JSON.stringify({
          key,
          version,
          bytes,
          mode: request.headers['x-amz-object-lock-mode'],
          retainUntil: request.headers['x-amz-object-lock-retain-until-date'],
        }),
      );
      response.setHeader('x-amz-version-id', version);
      response.setHeader('etag', `"${createHash('md5').update(body).digest('hex')}"`);
      response.end();
    } else if (request.method === 'GET') {
      try {
        const body = await readFile(file);
        response.setHeader('Content-Length', body.length);
        response.end(body);
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
        response.writeHead(404);
        response.end();
      }
    } else {
      response.writeHead(403);
      response.end('immutable fixture');
    }
  })().catch(() => {
    process.stderr.write('Fixture request failed\n');
    response.writeHead(500);
    response.end();
  });
}).listen(9000, '0.0.0.0');
