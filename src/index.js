import {Container} from '@cloudflare/containers';
import {connect} from 'cloudflare:sockets';
export class usxray extends Container {
    defaultPort = 8080;
    sleepAfter = '1h';
}
const bufferSize = 256 * 1024;
const startThreshold = 50 * 1024 * 1024;
const maxChunkLen = 64 * 1024;
const flushTime = 3;
const textDecoder = new TextDecoder();
const createConnect = (hostname, port, socket = connect({hostname, port})) => socket.opened.then(() => socket);
const concurrentConnect = (hostname, port) => {
    let settled = false, winner = null;
    const sockets = new Array(6);
    const closeSocket = socket => {try {socket?.close()} catch {}};
    const attempts = Array.from({length: 6}, (_, i) => {
        const socket = connect({hostname, port});
        sockets[i] = socket;
        return createConnect(hostname, port, socket).then(openedSocket => {
            if (settled && openedSocket !== winner) closeSocket(openedSocket);
            return openedSocket;
        });
    });
    return Promise.any(attempts).then(socket => {
        settled = true, winner = socket;
        for (const other of sockets) if (other !== socket) closeSocket(other);
        return socket;
    }, err => {
        settled = true;
        for (const socket of sockets) closeSocket(socket);
        throw err;
    });
};
const manualPipe = async (readable, writable, close) => {
    const safeBufferSize = bufferSize - maxChunkLen, fastFlushOffset = maxChunkLen << 1;
    let buffer = new ArrayBuffer(bufferSize), spareBuffer = new ArrayBuffer(maxChunkLen), bufferView = new Uint8Array(buffer);
    let offset = 0, totalBytes = 0, time = 0, timerId = null, resume = null, isReading = false, needsFlush = false, protectFlush = false;
    let isClose = false, fastFlush = true;
    const flushBuffer = () => {
        if (isReading) return needsFlush = true;
        fastFlush = offset < fastFlushOffset;
        if (offset > 0 && !isClose) {
            offset > safeBufferSize
                ? (writable.send(bufferView.subarray(0, offset)), buffer = new ArrayBuffer(bufferSize), bufferView = new Uint8Array(buffer))
                : writable.send(bufferView.slice(0, offset));
            offset = 0;
        }
        needsFlush = false, protectFlush = false, timerId && (clearTimeout(timerId), timerId = null), resume?.(), resume = null;
    };
    const reader = readable.getReader({mode: 'byob'});
    try {
        while (true) {
            const useSpare = offset > 0 && protectFlush;
            let readBuffer = buffer, readOffset = offset;
            isReading = offset > 0;
            useSpare && (readBuffer = spareBuffer, readOffset = 0, isReading = false);
            const {done, value} = await reader.read(new Uint8Array(readBuffer, readOffset, maxChunkLen));
            isReading = false;
            useSpare ? (bufferView.set(value, offset), spareBuffer = value.buffer) : (buffer = value.buffer, bufferView = new Uint8Array(buffer));
            if (done) break;
            const chunkLen = value.byteLength;
            if (!chunkLen) {
                needsFlush && flushBuffer();
                continue;
            }
            offset += chunkLen, totalBytes += chunkLen;
            if (needsFlush || chunkLen < 2048) {
                flushBuffer();
            } else {
                if (fastFlush || chunkLen < 28672) {
                    totalBytes = 0, time = 2;
                } else if (totalBytes > startThreshold) time = flushTime;
                timerId ||= setTimeout(flushBuffer, time), protectFlush = chunkLen < maxChunkLen;
                offset > safeBufferSize && (totalBytes > startThreshold ? await new Promise(r => resume = r) : flushBuffer());
            }
        }
    } catch {close?.(), isClose = true} finally {isReading = false, flushBuffer()}
};
const getEarlyData = request => {
    const refererHeader = request.headers.get('Referer');
    const protocolHeader = refererHeader || request.headers.get('sec-websocket-protocol');
    let earlyDataHeader = null;
    if (refererHeader) {
        earlyDataHeader = protocolHeader.slice(request.headers.get('host').length);
    } else if (protocolHeader) {
        earlyDataHeader = protocolHeader;
    }
    // @ts-ignore
    return earlyDataHeader ? Uint8Array.fromBase64(earlyDataHeader, {alphabet: 'base64url'}) : null;
};
const parseRequest = chunk => {
    if (chunk.byteLength < 24) return null;
    let offset = 19 + chunk[17];
    if (chunk.byteLength < offset + 3) return null;
    const port = (chunk[offset] << 8) | chunk[offset + 1];
    offset += 2;
    const addrType = chunk[offset++];
    let newOffset, hostname;
    if (addrType === 2) {
        if (chunk.byteLength < offset + 1) return null;
        const len = chunk[offset++];
        newOffset = offset + len;
        if (chunk.byteLength < newOffset) return null;
        hostname = textDecoder.decode(chunk.subarray(offset, newOffset));
    } else if (addrType === 1) {
        newOffset = offset + 4;
        if (chunk.byteLength < newOffset) return null;
        const bytes = chunk.subarray(offset, newOffset);
        hostname = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
    } else {
        newOffset = offset + 16;
        if (chunk.byteLength < newOffset) return null;
        let ipv6Str = ((chunk[offset] << 8) | chunk[offset + 1]).toString(16);
        for (let i = 1; i < 8; i++) ipv6Str += ':' + ((chunk[offset + i * 2] << 8) | chunk[offset + i * 2 + 1]).toString(16);
        hostname = `[${ipv6Str}]`;
    }
    return {version: chunk[0], hostname, port, payload: chunk.subarray(newOffset)};
};
const createBufferedTcpWriter = (tcpWriter, close) => {
    const queue = new Array(2048);
    let head = 0, tail = 0, size = 0, coalesceBuffer = null, drainActive = false, closed = false;
    const closeWriter = () => {
        if (closed) return;
        closed = true;
        for (let i = 0; i < 2048; i++) queue[i] = null;
        close?.();
    };
    const drainQueue = async () => {
        if (closed) return;
        try {
            while (size > 0 && !closed) {
                let chunk = queue[head];
                if (chunk.byteLength >= maxChunkLen) {
                    queue[head] = null, head = (head + 1) & 2047, size--;
                    await tcpWriter.write(chunk);
                    continue;
                }
                let mergedLength = 0;
                coalesceBuffer ||= new Uint8Array(maxChunkLen);
                while (size > 0) {
                    chunk = queue[head];
                    if (mergedLength + chunk.byteLength > maxChunkLen) break;
                    coalesceBuffer.set(chunk, mergedLength), mergedLength += chunk.byteLength;
                    queue[head] = null, head = (head + 1) & 2047, size--;
                }
                if (mergedLength > 0) await tcpWriter.write(coalesceBuffer.subarray(0, mergedLength));
            }
        } catch {closeWriter()} finally {drainActive = false}
    };
    return chunk => {
        if (closed) return;
        const data = chunk.constructor === Uint8Array ? chunk : new Uint8Array(chunk);
        if (!data.byteLength) return;
        if (size === 2048) return closeWriter();
        queue[tail] = data, tail = (tail + 1) & 2047, size++;
        if (!drainActive) drainActive = true, queueMicrotask(drainQueue);
    };
};
const handleWebSocketConn = (webSocket, tcpSocket, parsedRequest) => {
    const tcpWriter = tcpSocket.writable.getWriter();
    let isClosed = false;
    const close = () => {
        if (isClosed) return;
        isClosed = true;
        try { tcpSocket.close(); } catch {}
        try { webSocket.close(); } catch {}
    };
    const tcpWrite = createBufferedTcpWriter(tcpWriter, close);
    // @ts-ignore
    webSocket.accept({allowHalfOpen: true}), webSocket.binaryType = "arraybuffer";
    webSocket.addEventListener("message", event => {tcpWrite(event.data)});
    webSocket.addEventListener("close", close);
    webSocket.addEventListener("error", close);
    manualPipe(tcpSocket.readable, webSocket, close).finally(close);
    webSocket.send(new Uint8Array([parsedRequest.version, 0]));
    parsedRequest.payload.byteLength && tcpWrite(parsedRequest.payload);
};
export default {
    async fetch(request, env) {
        const container = env.USXRAY.getByName('default');
        if (request.headers.get('Upgrade') !== 'websocket') return new Response(null, {status: 404});
        if (request.url.includes('proxyall')) return container.fetch(request);
        const earlyData = getEarlyData(request);
        if (!earlyData) return container.fetch(request);
        const parsedRequest = parseRequest(earlyData);
        if (!parsedRequest) return container.fetch(request);
        const tcpSocket = await concurrentConnect(parsedRequest.hostname, parsedRequest.port).catch(() => null);
        if (!tcpSocket) return container.fetch(request);
        const {0: clientSocket, 1: webSocket} = new WebSocketPair();
        handleWebSocketConn(webSocket, tcpSocket, parsedRequest);
        return new Response(null, {status: 101, webSocket: clientSocket});
    }
};
