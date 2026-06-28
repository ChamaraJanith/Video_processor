require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// AWS Clients
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const sqs = new SQSClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Multer - store file in memory before uploading to S3
const upload = multer({ storage: multer.memoryStorage() });

// ─── Routes ────────────────────────────────────────────────────────────────────

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Upload video → S3 → Send SQS message
app.post('/upload', upload.single('video'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No video file provided' });
    }

    const jobId = uuidv4();
    const ext = path.extname(req.file.originalname) || '.mp4';
    const inputKey = `uploads/${jobId}/input${ext}`;

    // 1. Upload original video to S3
    console.log(`[Upload] Uploading ${req.file.originalname} to S3 as ${inputKey}`);
    await s3.send(new PutObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: inputKey,
      Body: req.file.buffer,
      ContentType: req.file.mimetype,
    }));
    console.log(`[Upload] S3 upload complete: ${inputKey}`);

    // 2. Send job message to SQS
    const jobMessage = {
      jobId,
      inputKey,
      bucket: process.env.S3_BUCKET_NAME,
      outputFormats: ['720p', '480p', '360p'],
      createdAt: new Date().toISOString(),
    };

    await sqs.send(new SendMessageCommand({
      QueueUrl: process.env.SQS_QUEUE_URL,
      MessageBody: JSON.stringify(jobMessage),
      MessageAttributes: {
        JobId: { DataType: 'String', StringValue: jobId },
      },
    }));
    console.log(`[Upload] SQS message sent for job: ${jobId}`);

    res.status(202).json({
      success: true,
      jobId,
      inputKey,
      message: 'Video uploaded successfully. Transcoding job queued.',
      formats: ['720p', '480p', '360p'],
    });

  } catch (err) {
    console.error('[Upload] Error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Get presigned URL to download a transcoded output
app.get('/download/:jobId/:format', async (req, res) => {
  try {
    const { jobId, format } = req.params;
    const outputKey = `outputs/${jobId}/${format}.mp4`;

    const url = await getSignedUrl(s3, new GetObjectCommand({
      Bucket: process.env.S3_BUCKET_NAME,
      Key: outputKey,
    }), { expiresIn: 3600 });

    res.json({ success: true, downloadUrl: url, expires: '1 hour' });
  } catch (err) {
    console.error('[Download] Error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Start Server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════╗
║   🎬 Video Transcoder API Server Running     ║
║   Port    : ${PORT}                              ║
║   Region  : ${process.env.AWS_REGION}                    ║
║   Bucket  : ${process.env.S3_BUCKET_NAME}  ║
╚══════════════════════════════════════════════╝
  `);
});
