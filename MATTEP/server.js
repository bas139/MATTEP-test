import { WebSocketServer } from 'ws';
import { Queue } from 'bullmq';
import { z } from 'zod';
import Redis from 'ioredis';
import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import os from 'os';
import http from 'http';

// 1. ตั้งค่า Redis และ Message Queue (พักข้อมูลชั่วคราว ไม่ให้ฐานข้อมูลรับโหลดหนักไป)
let redisConnection = null;
let eventQueue = null;
// console.log('⚠️ [ระบบ] ข้ามการเชื่อมต่อ Redis เพื่อให้เซิร์ฟเวอร์ทำงานได้โดยไม่มี Error'); // หากต้องการใช้งาน Redis ให้เปิดบรรทัดนี้และตั้งค่าการเชื่อมต่อ

// 2. Data Validation Schema ป้องกันคนเขียน Script ยิง API ปลอมเข้ามาป่วน
const EventPayloadSchema = z.object({
    uid: z.string().length(22).regex(/^[a-zA-Z0-9-_]+$/),
    type: z.enum(['GAZE', 'HEAD_POSE', 'TAB_SWITCH', 'LIVENESS', 'SUBMIT', 'SYSTEM']),
    data: z.record(z.any()).refine(obj => Object.keys(obj).length <= 50, { message: 'Too many data fields' }),
    timestamp: z.number().int().positive().refine(t => Math.abs(Date.now() - t) < 300000, { message: 'Timestamp too old' }) // ขยายเพดานเป็น 5 นาที (300,000ms)
});

// ===== แก้ไข: รวมร่าง Express และ WebSocket ให้อยู่ในพอร์ตเดียวกัน =====
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
// =====================================================================

// 3. Rate Limiting State เก็บไว้ใน Memory เพื่อความเร็วในการตรวจจับ
const rateLimits = new Map();
const MAX_MESSAGES_PER_SEC = 15; // ห้ามส่งข้อมูลรัวเกิน 15 ครั้งใน 1 วินาที
const MAX_CONNECTIONS = 1000; // จำกัดจำนวน connections
const CLEANUP_INTERVAL = 60000; // ทำความสะอาด rate limits ทุก 1 นาที

// Cleanup old rate limit entries
setInterval(() => {
    const now = Date.now();
    for (const [ip, state] of rateLimits.entries()) {
        if (now - state.lastReset > 300000) { // ลบ entries ที่เก่าเกิน 5 นาที
            rateLimits.delete(ip);
        }
    }
    console.log(`[Cleanup] Active connections: ${wss.clients.size}, Rate limit entries: ${rateLimits.size}`);
}, CLEANUP_INTERVAL);

wss.on('connection', (ws, req) => {
    if (wss.clients.size > MAX_CONNECTIONS) {
        ws.close(1008, 'Server capacity exceeded');
        return;
    }

    const clientIp = req.socket.remoteAddress || 'unknown';
    const limitState = { count: 0, lastReset: Date.now() };
    rateLimits.set(clientIp, limitState);

    ws.on('message', async (rawMessage) => {
        try {
            const now = Date.now();

            // 4. Rate Limiting Logic (ด่านตรวจจับสแปม)
            if (now - limitState.lastReset > 1000) {
                limitState.count = 0;
                limitState.lastReset = now;
            }
            if (limitState.count >= MAX_MESSAGES_PER_SEC) {
                ws.send(JSON.stringify({ error: 'RATE_LIMIT_EXCEEDED', code: 429 }));
                return; // บล็อกทันที ไม่ให้ส่งข้อมูลเข้าระบบ
            }
            limitState.count++;

            // 5. Message Size Check (ป้องกัน Buffer overflow)
            if (rawMessage.length > 1024 * 10) { // 10KB max
                ws.send(JSON.stringify({ error: 'PAYLOAD_TOO_LARGE', code: 413 }));
                return;
            }

            // 6. Schema Validation (กรอง Data ให้สะอาดที่สุด)
            let parsedData;
            try {
                parsedData = JSON.parse(rawMessage);
            } catch (parseErr) {
                ws.send(JSON.stringify({ error: 'INVALID_JSON', code: 400 }));
                return;
            }

            const validData = EventPayloadSchema.parse(parsedData);

            // 7. โยนเข้า Message Queue ทันที
            if (eventQueue && redisConnection && redisConnection.status === 'ready') {
                eventQueue.add('process-event', validData, {
                    removeOnComplete: true,
                    attempts: 3,
                    backoff: { type: 'exponential', delay: 2000 },
                    timeout: 5000
                }).catch(err => console.error('[Queue Error]', err.message));
            }

            ws.send(JSON.stringify({ status: 'ok', code: 200 }));
        } catch (error) {
            if (error instanceof z.ZodError) {
                ws.send(JSON.stringify({ error: 'VALIDATION_FAILED', details: error.errors.slice(0, 3), code: 422 }));
            } else {
                console.error('[Event Processing Error]', error.message);
                ws.send(JSON.stringify({ error: 'INTERNAL_ERROR', code: 500 }));
            }
        }
    });

    ws.on('error', (err) => {
        console.error(`[WebSocket Error from ${clientIp}]`, err.message);
    });

    ws.on('close', () => {
        rateLimits.delete(clientIp);
    });
});

wss.on('error', (err) => {
    console.error('[WebSocket Server Error]', err.message);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// เปิดใช้งาน CORS ให้หน้าเว็บเรียกข้ามโดเมนได้
app.use(cors());

// ขยายขนาด Payload เป็น 50mb เพราะข้อมูลที่ส่งมามีรูปถ่ายหลักฐาน AI (Base64) รวมอยู่ด้วย
app.use(express.json({ limit: '50mb' }));

// ====== สั่งให้เซิร์ฟเวอร์นำไฟล์หน้าเว็บ (index.html, JS, CSS) ไปแสดงผล ======
app.use(express.static(__dirname));

// ===== ฟังก์ชันตรวจสอบการดัดแปลงข้อมูล (Data Integrity Check) =====
const ANSWER_KEY_SALT = 'mattep-v4-local-signature';

function stableStringify(value) {
  if (value === undefined) return undefined;
  if (Array.isArray(value)) return '[' + value.map(v => stableStringify(v) ?? 'null').join(',') + ']';
  if (value && typeof value === 'object') {
    return '{' + Object.keys(value).filter(k => k !== 'integrity' && value[k] !== undefined).sort().map(k => JSON.stringify(k) + ':' + stableStringify(value[k])).join(',') + '}';
  }
  return JSON.stringify(value);
}

// API สำหรับดึงข้อมูลข้อสอบ (เชื่อมให้ Dashboard ตรงกับฐานข้อมูลหลักเสมอ)
app.get('/api/exam-config', (req, res) => {
    res.json([
        { text: "ภาษา HTML ใช้ทำอะไร?", correct: "B" },
        { text: "CSS มีหน้าที่อะไร?", correct: "A" },
        { text: "JavaScript ใช้ในการทำอะไร?", correct: "A" },
        { text: "ตัวแปร (Variable) คืออะไร?", correct: "A" },
        { text: "Loop ใช้สำหรับการทำอะไร?", correct: "A" },
        { text: "Function มีประโยชน์อะไร?", correct: "A" },
        { text: "Database ใช้เพื่อการอะไร?", correct: "B" },
        { text: "API ในการเขียนโปรแกรมคืออะไร?", correct: "A" }
    ]);
});

function createIntegrityHash(record) {
  const text = stableStringify(record) + ANSWER_KEY_SALT;
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}
// ==========================================================

// ฐานข้อมูลชั่วคราวสำหรับเก็บสถานะ Live ของผู้สอบ
const liveStudents = new Map();

// ลบข้อมูล Live ที่ค้างเกิน 1 นาที (กรณีเด็กปิดแท็บหรือหลุด)
setInterval(() => {
  const now = Date.now();
  for (const [uid, data] of liveStudents.entries()) {
    if (now - (data.lastUpdate || 0) > 60000) liveStudents.delete(uid);
  }
}, 60000);

// API สำหรับรับสถานะแบบ Real-time จากหน้าสอบ
app.post('/api/live-sync', (req, res) => {
  const data = req.body;
  if (data && data.uid) {
    data.lastUpdate = Date.now();
    liveStudents.set(data.uid, data);
  }
  res.json({ success: true });
});

// โฟลเดอร์สำหรับเก็บไฟล์ข้อสอบที่เด็กส่งมา
const UPLOADS_DIR = path.join(__dirname, 'submissions');
if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR);
}

// API สำหรับรับข้อสอบ
app.post('/api/submit-exam', async (req, res) => {
  const record = req.body;

  if (!record || !record.uid) {
    return res.status(400).json({ error: 'ข้อมูลไม่ครบถ้วน' });
  }

  // 1. ตรวจสอบว่ามีการแอบแก้ไขข้อมูลกลางทาง (Tampering) หรือไม่
  const expectedHash = createIntegrityHash(record);
  if (expectedHash !== record.integrity) {
    console.warn(`[🚨 ALERT] ตรวจพบการดัดแปลงข้อมูลจากผู้สอบ: ${record.name}`);
    return res.status(403).json({ error: 'การตรวจสอบความปลอดภัยล้มเหลว (Data Tampering Detected)' });
  }

  try {
    // 2. บันทึกข้อมูลลงฐานข้อมูล (ในที่นี้บันทึกเป็นไฟล์ .json แยกตามคนเพื่อความง่าย)
    const fileName = `submit_${record.uid}_${Date.now()}.json`;
    const savePath = path.join(UPLOADS_DIR, fileName);
    
    await fs.promises.writeFile(savePath, JSON.stringify(record, null, 2));

    // เมื่อสอบเสร็จ ให้ลบออกจากระบบ Live ทันที
    liveStudents.delete(record.uid);

    const totalCheatScore = record.totalRiskScore !== undefined ? record.totalRiskScore : Math.min(100, (record.suspicionScore || 0) + (record.aiScore || 0));
    console.log(`[SUCCESS] รับข้อสอบจาก: ${record.name} (คะแนน: ${record.score}/${record.scoreMax}) | ความเสี่ยงทุจริตรวม: ${totalCheatScore} แต้ม (ระบบ ${record.suspicionScore || 0}, AI ${record.aiScore || 0})`);
    
    res.json({ success: true, message: 'ส่งข้อสอบสำเร็จ' });
  } catch (err) {
    console.error('[ERROR] ไม่สามารถบันทึกข้อสอบได้:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// API สำหรับส่งข้อมูลทั้งหมดไปให้หน้า Dashboard
app.get('/api/dashboard-data', async (req, res) => {
  const finished = [];
  try {
    const files = await fs.promises.readdir(UPLOADS_DIR);
    for (const file of files) {
      if (file.endsWith('.json')) {
        const content = await fs.promises.readFile(path.join(UPLOADS_DIR, file), 'utf-8');
        try { finished.push(JSON.parse(content)); } catch(e) {}
      }
    }
  } catch (err) {
    console.error('[ERROR] อ่านไฟล์ submissions ไม่ได้:', err);
  }
  
  res.json({
    live: Array.from(liveStudents.values()),
    finished: finished
  });
});

// เริ่มต้นเปิดเซิร์ฟเวอร์
const PORT = process.env.PORT || 3000; 
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 เซิร์ฟเวอร์ออนไลน์บน Render แล้วที่พอร์ต: ${PORT}`);
});