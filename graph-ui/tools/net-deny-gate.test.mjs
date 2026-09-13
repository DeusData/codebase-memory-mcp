import { describe, expect, it } from 'vitest';
import { classifyLine, isLoopbackHost, splitHostPort } from './net-deny-gate.mjs';

/*
 * Die Zeilen sind echte Ausgaben von `lsof -a -i -n -P` auf dieser Maschine,
 * woertlich uebernommen. Ohne diese Tests waere "outboundViolations: 0" wertlos:
 * ein Klassifikator, der nie etwas findet, meldet dieselbe Null wie eine saubere
 * Maschine. Hier wird bewiesen, dass er sehr wohl etwas findet.
 */

const LOOPBACK_UDP =
  'postgres  32120 bernhard   10u  IPv6 0x833a2aa804e3c858      0t0  UDP [::1]:64649->[::1]:64649';
const EXTERNAL_TCP =
  'Google    81978 bernhard   30u  IPv4 0xe75e15a87b455496      0t0  TCP 192.168.178.32:64257->140.82.121.3:443 (ESTABLISHED)';
const EXTERNAL_TCP_V6 =
  'sample.ex  3565 bernhard    7u  IPv6 0x5917c636b8a90748      0t0  TCP [2a00:1f:9082:3601:3d10:37f5:dd94:87ce]:64021->[2607:6bc0::10]:443 (ESTABLISHED)';
const LISTEN_LOCAL =
  'mongod     3098 bernhard    9u  IPv4  0x1684421c05b3fd5      0t0  TCP 127.0.0.1:27017 (LISTEN)';
const LISTEN_ANY =
  'ControlCe   652 bernhard    8u  IPv4 0x278b381fd868a8bf      0t0  TCP *:7000 (LISTEN)';
const UDP_UNCONNECTED =
  'sharingd    647 bernhard    4u  IPv4 0x9f08eec658eac830      0t0  UDP *:*';
const LOOPBACK_TCP =
  'node      12345 bernhard   21u  IPv4 0x1111111111111111      0t0  TCP 127.0.0.1:53001->127.0.0.1:4200 (ESTABLISHED)';

describe('isLoopbackHost', () => {
  it('erkennt die Schreibweisen von localhost', () => {
    for (const host of ['127.0.0.1', '127.0.0.2', 'localhost', '::1', '[::1]', '::ffff:127.0.0.1']) {
      expect(isLoopbackHost(host), host).toBe(true);
    }
  });

  it('haelt alles andere fuer auswaerts, auch das eigene LAN', () => {
    for (const host of ['192.168.178.32', '10.0.0.1', '140.82.121.3', '[2607:6bc0::10]', '0.0.0.0']) {
      expect(isLoopbackHost(host), host).toBe(false);
    }
  });
});

describe('splitHostPort', () => {
  it('trennt IPv4 am letzten Doppelpunkt', () => {
    expect(splitHostPort('140.82.121.3:443')).toEqual({ host: '140.82.121.3', port: '443' });
  });

  it('haelt die Klammern von IPv6 zusammen', () => {
    expect(splitHostPort('[2607:6bc0::10]:443')).toEqual({ host: '[2607:6bc0::10]', port: '443' });
  });
});

describe('classifyLine', () => {
  it('meldet eine Verbindung nach draussen als nicht loopback', () => {
    const socket = classifyLine(EXTERNAL_TCP);
    expect(socket).not.toBeNull();
    expect(socket.protocol).toBe('TCP');
    expect(socket.remoteHost).toBe('140.82.121.3');
    expect(socket.remotePort).toBe('443');
    expect(socket.loopback).toBe(false);
    expect(socket.pid).toBe(81978);
  });

  it('meldet auch eine IPv6-Verbindung nach draussen', () => {
    const socket = classifyLine(EXTERNAL_TCP_V6);
    expect(socket.loopback).toBe(false);
    expect(socket.remoteHost).toBe('[2607:6bc0::10]');
  });

  it('erkennt Loopback-Verbindungen als erlaubt', () => {
    expect(classifyLine(LOOPBACK_TCP).loopback).toBe(true);
    expect(classifyLine(LOOPBACK_UDP).loopback).toBe(true);
  });

  it('ignoriert lauschende Sockets, die kein Gegenueber haben', () => {
    expect(classifyLine(LISTEN_LOCAL)).toBeNull();
    expect(classifyLine(LISTEN_ANY)).toBeNull();
    expect(classifyLine(UDP_UNCONNECTED)).toBeNull();
  });

  it('ignoriert die Kopfzeile von lsof', () => {
    expect(classifyLine('COMMAND     PID     USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME')).toBeNull();
  });
});
