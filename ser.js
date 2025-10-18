const express = require("express");
const { Pool } = require("pg");
const multer = require("multer");
const { v4: uuidv4 } = require("uuid");
const crypto = require("crypto");
const http = require("http");
const { Readable, Transform, PassThrough } = require("stream");
const { pipeline } = require("stream/promises");
require("dotenv").config();
const fetch = require("node-fetch");
const cluster = require("cluster");
const os = require("os");
const fs = require("fs");
const path = require("path");

// ============================================================================= 
// CONFIGURATION CONSTANTS
// =============================================================================
const MAX_DB_CAPACITY_MB = 450;
const DB_ROTATION_THRESHOLD = 400;
const MAX_CHUNK_SIZE = 500 * 1024 * 1024; // 200MB
const MIN_CHUNK_SIZE = 50 * 1024 * 1024;  // 20MB
const MESSAGE_CHUNK_SIZE = 500 * 1024;      // 8KB
const DROPBOX_ACCOUNT_MAX_CAPACITY = 1.8 * 1024 * 1024 * 1024; // 1.8GB
const DROPBOX_ROTATION_THRESHOLD = 1.5 * 1024 * 1024 * 1024;   // 1.5GB
const PROGRESS_UPDATE_INTERVAL = 1024;    // 1KB intervals
const PROGRESS_UPDATE_FREQUENCY_MS = 100; // 100ms updates
const MAX_CONCURRENT_UPLOADS = Math.max(2, os.cpus().length);
const UPLOAD_TIMEOUT = 300000;            // 5 minutes
const RETRY_ATTEMPTS = 3;

// Temp directory configuration
const SYSTEM_TEMP_BASE = os.tmpdir();
const APP_TEMP_DIR = path.join(SYSTEM_TEMP_BASE, 'infinity-share-uploads');
const USER_TEMP_DIR = path.join(APP_TEMP_DIR, `user-${process.pid}-${Date.now()}`);

// Initialize temp directories
function initializeTempDirectories() {
  try {
    if (!fs.existsSync(APP_TEMP_DIR)) {
      fs.mkdirSync(APP_TEMP_DIR, { recursive: true });
      console.log(`[TempDir] Created base temp directory: ${APP_TEMP_DIR}`);
    }

    if (!fs.existsSync(USER_TEMP_DIR)) {
      fs.mkdirSync(USER_TEMP_DIR, { recursive: true });
      console.log(`[TempDir] Created user temp directory: ${USER_TEMP_DIR}`);
    }

    if (process.platform !== 'win32') {
      fs.chmodSync(USER_TEMP_DIR, 0o755);
    }

    console.log(`[TempDir] Using temp directory: ${USER_TEMP_DIR}`);
    return USER_TEMP_DIR;
  } catch (error) {
    console.error(`[TempDir] Error creating directories:`, error.message);
    const fallbackDir = path.join(SYSTEM_TEMP_BASE, `infinity-temp-${process.pid}`);
    try {
      if (!fs.existsSync(fallbackDir)) {
        fs.mkdirSync(fallbackDir, { recursive: true });
      }
      console.log(`[TempDir] Using fallback directory: ${fallbackDir}`);
      return fallbackDir;
    } catch (fallbackError) {
      console.error(`[TempDir] Fallback failed:`, fallbackError.message);
      return SYSTEM_TEMP_BASE;
    }
  }
}

const TEMP_UPLOAD_DIR = initializeTempDirectories();

// Cluster setup
const numCPUs = Math.min(os.cpus().length, 4);

if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  console.log(`[Cluster] Master ${process.pid} starting ${numCPUs} workers`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`[Cluster] Worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  // startServer() call moved to end of file after all class definitions
}

// =============================================================================
// SERVER-SENT EVENTS (SSE) CONFIGURATION - GLOBAL CLASSES
// =============================================================================

// SSE Client Connection Manager - Cloud Optimized
class SSEConnectionManager {
  constructor() {
    this.instanceId = Date.now() + '-' + Math.random().toString(36).substr(2, 9);
    console.log(`[SSEConnectionManager] Creating new instance with ID: ${this.instanceId}`);
    this.connections = new Map(); // userId -> { res, roomName, lastHeartbeat, userId }
    this.roomConnections = new Map(); // roomName -> Set of userIds
    this.heartbeatInterval = 15000; // 15 seconds for cloud deployment
    this.connectionTimeout = 45000; // 45 seconds timeout
    this.keepAliveInterval = 5000; // 5 seconds keep-alive for cloud platforms
    this.startHeartbeat();
    this.startKeepAlive();
    this.startCleanup();
    console.log(`[SSEConnectionManager] Instance ${this.instanceId} initialized with connections and rooms`);
  }

  addConnection(userId, roomName, res) {
    console.log('='.repeat(60));
    console.log(`[SSE-MANAGER] Instance ${this.instanceId} - Adding new connection for user "${userId}" in room "${roomName}"`);
    console.log(`[SSE-MANAGER] Timestamp: ${new Date().toISOString()}`);
    console.log(`[SSE-MANAGER] Response object status:`);
    console.log(`[SSE-MANAGER] - headersSent: ${res.headersSent}`);
    console.log(`[SSE-MANAGER] - finished: ${res.finished}`);
    console.log(`[SSE-MANAGER] - destroyed: ${res.destroyed}`);
    console.log(`[SSE-MANAGER] - writable: ${res.writable}`);
    console.log(`[SSE-MANAGER] - writableEnded: ${res.writableEnded}`);

    // Clean up any existing connection for this user
    const existingConnection = this.connections.get(userId);
    if (existingConnection) {
      console.log(`[SSE-MANAGER] Found existing connection for user "${userId}", cleaning up...`);
      console.log(`[SSE-MANAGER] - Existing room: "${existingConnection.roomName}"`);
      console.log(`[SSE-MANAGER] - Existing connected: ${existingConnection.connected}`);
      console.log(`[SSE-MANAGER] - Existing lastHeartbeat: ${new Date(existingConnection.lastHeartbeat).toISOString()}`);
      this.removeConnection(userId);
      console.log(`[SSE-MANAGER] Existing connection cleaned up`);
    } else {
      console.log(`[SSE-MANAGER] No existing connection found for user "${userId}"`);
    }

    // Log current connection state before adding
    console.log(`[SSE-MANAGER] Current connections before adding: ${this.connections.size}`);
    console.log(`[SSE-MANAGER] Current rooms before adding: ${this.roomConnections.size}`);
    if (this.roomConnections.has(roomName)) {
      console.log(`[SSE-MANAGER] Room "${roomName}" currently has ${this.roomConnections.get(roomName).size} users`);
    } else {
      console.log(`[SSE-MANAGER] Room "${roomName}" does not exist yet`);
    }

    // Only set headers if they haven't been sent yet
    if (!res.headersSent) {
      console.log(`[SSE-MANAGER] Headers not sent yet, setting additional SSE headers...`);
      try {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Headers': 'Cache-Control'
        });
        console.log(`[SSE-MANAGER] Additional SSE headers set successfully`);
      } catch (headerError) {
        console.error(`[SSE-MANAGER] Failed to set additional headers:`, headerError.message);
      }
    } else {
      console.log(`[SSE-MANAGER] Headers already sent, skipping header setup`);
    }

    // Send initial connection message
    console.log(`[SSE-MANAGER] Sending initial 'connected' event...`);
    try {
      const connectionEventSent = this.sendEvent(res, 'connected', {
        userId,
        roomName,
        timestamp: Date.now(),
        message: 'SSE connection established',
        connectionId: `${userId}-${Date.now()}`
      });

      if (connectionEventSent) {
        console.log(`[SSE-MANAGER] Initial 'connected' event sent successfully`);
      } else {
        console.error(`[SSE-MANAGER] Failed to send initial 'connected' event`);
      }
    } catch (eventError) {
      console.error(`[SSE-MANAGER] Error sending initial connected event:`, eventError.message);
    }

    const connection = {
      res,
      roomName,
      userId,
      lastHeartbeat: Date.now(),
      connected: true,
      createdAt: Date.now(),
      eventsSent: 1 // Initial connected event
    };

    console.log(`[SSE-MANAGER] Connection object created:`, {
      userId: connection.userId,
      roomName: connection.roomName,
      connected: connection.connected,
      createdAt: new Date(connection.createdAt).toISOString(),
      lastHeartbeat: new Date(connection.lastHeartbeat).toISOString()
    });

    // Store connection
    try {
      this.connections.set(userId, connection);
      console.log(`[SSE-MANAGER] Connection stored in connections map`);
      console.log(`[SSE-MANAGER] Total connections now: ${this.connections.size}`);
    } catch (storeError) {
      console.error(`[SSE-MANAGER] Failed to store connection:`, storeError.message);
      throw storeError;
    }

    // Add to room connections
    try {
      if (!this.roomConnections.has(roomName)) {
        console.log(`[SSE-MANAGER] Creating new room "${roomName}"`);
        this.roomConnections.set(roomName, new Set());
        console.log(`[SSE-MANAGER] New room created, total rooms: ${this.roomConnections.size}`);
      } else {
        console.log(`[SSE-MANAGER] Room "${roomName}" already exists`);
      }

      this.roomConnections.get(roomName).add(userId);
      const roomUserCount = this.roomConnections.get(roomName).size;
      console.log(`[SSE-MANAGER] User added to room, room now has ${roomUserCount} users`);
      console.log(`[SSE-MANAGER] Room users: [${Array.from(this.roomConnections.get(roomName)).join(', ')}]`);

    } catch (roomError) {
      console.error(`[SSE-MANAGER] Failed to add user to room:`, roomError.message);
      // Clean up connection if room setup fails
      this.connections.delete(userId);
      throw roomError;
    }

    // Handle client disconnect
    res.on('close', () => {
      console.log(`[SSE-MANAGER] Client disconnect event for user "${userId}" in room "${roomName}"`);
      this.removeConnection(userId);
    });

    res.on('error', (error) => {
      console.error(`[SSE-MANAGER] Connection error for user "${userId}" in room "${roomName}":`, error.message);
      console.error(`[SSE-MANAGER] Error details:`, {
        code: error.code,
        errno: error.errno,
        syscall: error.syscall,
        stack: error.stack
      });
      this.removeConnection(userId);
    });

    res.on('finish', () => {
      console.log(`[SSE-MANAGER] Response finished for user "${userId}" in room "${roomName}"`);
    });

    res.on('drain', () => {
      console.log(`[SSE-MANAGER] Response drained for user "${userId}"`);
    });

    console.log(`[SSE-MANAGER] Event handlers attached successfully`);
    console.log(`[SSE-MANAGER] Client "${userId}" connected to room "${roomName}" - CONNECTION COMPLETE`);
    console.log('='.repeat(60));

    return connection;
  }

  removeConnection(userId) {
    const connection = this.connections.get(userId);
    if (connection) {
      const { roomName } = connection;
      this.connections.delete(userId);

      // Remove from room connections
      const roomUsers = this.roomConnections.get(roomName);
      if (roomUsers) {
        roomUsers.delete(userId);
        if (roomUsers.size === 0) {
          this.roomConnections.delete(roomName);
        }
      }

      console.log(`[SSE] Client ${userId} disconnected from room ${roomName}`);
    }
  }

  broadcast(roomName, eventType, data, excludeUserId = null) {
    console.log(`[SSE-BROADCAST] Instance ${this.instanceId} - Broadcasting "${eventType}" to room "${roomName}"`);
    console.log(`[SSE-BROADCAST] Event data:`, JSON.stringify(data, null, 2));
    console.log(`[SSE-BROADCAST] Exclude user: ${excludeUserId || 'none'}`);

    const roomUsers = this.roomConnections.get(roomName);
    if (!roomUsers || roomUsers.size === 0) {
      console.log('='.repeat(60));
      console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - ⚠️  NO CONNECTIONS FOR ROOM "${roomName}"`);
      console.warn(`[SSE-BROADCAST] Event "${eventType}" will not be delivered to anyone`);
      console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - Total rooms with connections: ${this.roomConnections.size}`);
      console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - Existing rooms:`, Array.from(this.roomConnections.keys()));
      console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - Total active connections: ${this.connections.size}`);
      if (this.connections.size > 0) {
        console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - Active users:`, Array.from(this.connections.keys()));
        // Log which rooms these users are in
        for (const [userId, connection] of this.connections) {
          console.warn(`[SSE-BROADCAST] Instance ${this.instanceId} - User "${userId}" is in room "${connection.roomName}"`);
        }
      }
      console.log('='.repeat(60));
      return;
    }

    console.log(`[SSE-BROADCAST] Room "${roomName}" has ${roomUsers.size} users: [${Array.from(roomUsers).join(', ')}]`);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    let disconnectedCount = 0;

    for (const userId of roomUsers) {
      if (excludeUserId && userId === excludeUserId) {
        console.log(`[SSE-BROADCAST] Skipping excluded user "${userId}"`);
        skippedCount++;
        continue;
      }

      const connection = this.connections.get(userId);
      if (!connection) {
        console.error(`[SSE-BROADCAST] No connection object found for user "${userId}" in room "${roomName}"`);
        disconnectedCount++;
        continue;
      }

      if (!connection.connected) {
        console.error(`[SSE-BROADCAST] Connection for user "${userId}" is marked as disconnected`);
        disconnectedCount++;
        continue;
      }

      console.log(`[SSE-BROADCAST] Sending "${eventType}" to user "${userId}"...`);

      try {
        const sent = this.sendEvent(connection.res, eventType, data);
        if (sent) {
          connection.eventsSent = (connection.eventsSent || 0) + 1;
          console.log(`[SSE-BROADCAST] ✅ Successfully sent to "${userId}" (total events sent: ${connection.eventsSent})`);
          successCount++;
        } else {
          console.error(`[SSE-BROADCAST] ❌ Failed to send to "${userId}" (sendEvent returned false)`);
          errorCount++;
        }
      } catch (error) {
        console.error(`[SSE-BROADCAST] ❌ Error sending to "${userId}": ${error.message}`);
        console.error(`[SSE-BROADCAST] Error details for "${userId}":`, {
          code: error.code,
          errno: error.errno,
          syscall: error.syscall
        });
        this.removeConnection(userId);
        errorCount++;
      }
    }

    console.log('='.repeat(60));
    console.log(`[SSE-BROADCAST] Broadcast "${eventType}" to room "${roomName}" COMPLETE`);
    console.log(`[SSE-BROADCAST] Results: ${successCount} success, ${errorCount} errors, ${skippedCount} skipped, ${disconnectedCount} disconnected`);
    console.log(`[SSE-BROADCAST] Room "${roomName}" effective delivery: ${successCount}/${roomUsers.size - skippedCount} users`);
    console.log('='.repeat(60));
  }

  sendToUser(userId, eventType, data) {
    const connection = this.connections.get(userId);
    if (connection && connection.connected) {
      try {
        this.sendEvent(connection.res, eventType, data);
        return true;
      } catch (error) {
        console.error(`[SSE] Error sending to ${userId}:`, error.message);
        this.removeConnection(userId);
      }
    }
    return false;
  }

  sendEvent(res, eventType, data) {
    // Check response status first
    if (res.destroyed) {
      console.error(`[SSE-SEND] Cannot send "${eventType}": response is destroyed`);
      return false;
    }

    if (res.writableEnded) {
      console.error(`[SSE-SEND] Cannot send "${eventType}": response writable ended`);
      return false;
    }

    if (!res.writable) {
      console.error(`[SSE-SEND] Cannot send "${eventType}": response not writable`);
      return false;
    }

    try {
      console.log(`[SSE-SEND] Preparing to send event "${eventType}"`);

      const eventData = JSON.stringify(data);
      const message = `event: ${eventType}\ndata: ${eventData}\n\n`;

      console.log(`[SSE-SEND] Event message prepared (${message.length} chars)`);
      console.log(`[SSE-SEND] Event content preview: ${message.substring(0, 200)}${message.length > 200 ? '...' : ''}`);

      // Attempt to write the message
      const writeResult = res.write(message);

      if (writeResult === false) {
        console.warn(`[SSE-SEND] Write returned false for event "${eventType}" - buffer full, but event queued`);
      } else {
        console.log(`[SSE-SEND] ✅ Event "${eventType}" written successfully`);
      }

      // Force flush if available
      if (res.flush && typeof res.flush === 'function') {
        try {
          res.flush();
          console.log(`[SSE-SEND] Response flushed successfully`);
        } catch (flushError) {
          console.warn(`[SSE-SEND] Flush failed:`, flushError.message);
        }
      }

      return true;
    } catch (error) {
      console.error(`[SSE-SEND] ❌ Error sending event "${eventType}":`, error.message);
      console.error(`[SSE-SEND] Error details:`, {
        code: error.code,
        errno: error.errno,
        syscall: error.syscall,
        destroyed: res.destroyed,
        writableEnded: res.writableEnded,
        writable: res.writable
      });
      return false;
    }
  }

  startHeartbeat() {
    setInterval(() => {
      const now = Date.now();
      for (const [userId, connection] of this.connections) {
        try {
          this.sendEvent(connection.res, 'heartbeat', { timestamp: now });
          connection.lastHeartbeat = now;
        } catch (error) {
          console.error(`[SSE] Heartbeat failed for ${userId}:`, error.message);
          this.removeConnection(userId);
        }
      }
    }, this.heartbeatInterval);
  }

  startCleanup() {
    setInterval(() => {
      const now = Date.now();
      const staleConnections = [];

      for (const [userId, connection] of this.connections) {
        if (now - connection.lastHeartbeat > this.connectionTimeout) {
          staleConnections.push(userId);
        }
      }

      staleConnections.forEach(userId => {
        console.log(`[SSE] Cleaning up stale connection: ${userId}`);
        this.removeConnection(userId);
      });
    }, this.connectionTimeout / 2);
  }

  // Cloud platform compatibility - frequent keep-alive signals
  startKeepAlive() {
    setInterval(() => {
      for (const [userId, connection] of this.connections) {
        try {
          // Send minimal keep-alive signal to prevent proxy timeouts
          if (!connection.res.destroyed && !connection.res.writableEnded) {
            connection.res.write(': keep-alive\n\n');
            // Force flush for immediate transmission
            if (connection.res.flush) {
              connection.res.flush();
            }
          }
        } catch (error) {
          console.error(`[SSE] Keep-alive failed for ${userId}:`, error.message);
          this.removeConnection(userId);
        }
      }
    }, this.keepAliveInterval);
  }

  getStats() {
    const roomStats = {};
    for (const [roomName, users] of this.roomConnections) {
      roomStats[roomName] = users.size;
    }

    return {
      totalConnections: this.connections.size,
      totalRooms: this.roomConnections.size,
      roomStats
    };
  }
}

// Polling Fallback Manager
class PollingFallbackManager {
  constructor() {
    this.userStates = new Map(); // userId -> { events: [], lastPoll: timestamp }
    this.cleanupInterval = 300000; // 5 minutes
    this.eventTTL = 600000; // 10 minutes
    this.startCleanup();
  }

  addEvent(userId, eventType, data) {
    if (!this.userStates.has(userId)) {
      this.userStates.set(userId, { events: [], lastPoll: Date.now() });
    }

    const userState = this.userStates.get(userId);
    userState.events.push({
      eventType,
      data,
      timestamp: Date.now(),
      id: require('uuid').v4()
    });

    // Keep only recent events (max 100 per user)
    if (userState.events.length > 100) {
      userState.events = userState.events.slice(-100);
    }
  }

  getEvents(userId, since = 0) {
    const userState = this.userStates.get(userId);
    if (!userState) {
      return [];
    }

    userState.lastPoll = Date.now();

    // Return events newer than 'since' timestamp
    return userState.events.filter(event => event.timestamp > since);
  }

  startCleanup() {
    setInterval(() => {
      const now = Date.now();
      const staleUsers = [];

      for (const [userId, userState] of this.userStates) {
        if (now - userState.lastPoll > this.eventTTL) {
          staleUsers.push(userId);
        } else {
          // Clean old events
          userState.events = userState.events.filter(
            event => now - event.timestamp < this.eventTTL
          );
        }
      }

      staleUsers.forEach(userId => {
        this.userStates.delete(userId);
      });
    }, this.cleanupInterval);
  }
}

// Real-time Event Broadcaster - Replaces Pusher functionality
class RealTimeEventBroadcaster {
  constructor() {
    this.instanceId = Date.now() + '-' + Math.random().toString(36).substr(2, 9);
    console.log(`[RealTimeEventBroadcaster] Creating new instance with ID: ${this.instanceId}`);
    console.log('[RealTimeEventBroadcaster] Creating new SSE Connection Manager instance...');
    this.sseManager = new SSEConnectionManager();
    this.pollingManager = new PollingFallbackManager();
    console.log(`[RealTimeEventBroadcaster] Instance ${this.instanceId} - SSE Connection Manager and Polling Manager created`);
  }

  // Equivalent to pusher.trigger()
  broadcast(roomName, eventType, data, excludeUserId = null) {
    console.log('='.repeat(80));
    console.log(`[REAL-TIME-BROADCASTER] Instance ${this.instanceId} - Broadcasting event "${eventType}" to room "${roomName}"`);
    console.log(`[REAL-TIME-BROADCASTER] Timestamp: ${new Date().toISOString()}`);
    console.log(`[REAL-TIME-BROADCASTER] Exclude user: ${excludeUserId || 'none'}`);
    console.log(`[REAL-TIME-BROADCASTER] Event data:`, JSON.stringify(data, null, 2));

    // Check if SSE manager exists
    if (!this.sseManager) {
      console.error(`[REAL-TIME-BROADCASTER] ❌ Instance ${this.instanceId} - SSE Manager not initialized!`);
      return;
    }

    console.log(`[REAL-TIME-BROADCASTER] Instance ${this.instanceId} - SSE Manager ${this.sseManager.instanceId} status:`, {
      totalConnections: this.sseManager.connections.size,
      totalRooms: this.sseManager.roomConnections.size,
      hasRoom: this.sseManager.roomConnections.has(roomName)
    });

    // Broadcast via SSE to connected clients
    console.log(`[REAL-TIME-BROADCASTER] Instance ${this.instanceId} - Broadcasting via SSE Manager ${this.sseManager.instanceId}...`);
    this.sseManager.broadcast(roomName, eventType, data, excludeUserId);

    // Store for polling clients in the same room
    const roomUsers = this.sseManager.roomConnections.get(roomName);
    if (roomUsers && roomUsers.size > 0) {
      console.log(`[REAL-TIME-BROADCASTER] Storing events for polling fallback (${roomUsers.size} users)...`);
      let pollingEventsStored = 0;

      for (const userId of roomUsers) {
        if (excludeUserId && userId === excludeUserId) {
          console.log(`[REAL-TIME-BROADCASTER] Skipping polling storage for excluded user "${userId}"`);
          continue;
        }

        try {
          this.pollingManager.addEvent(userId, eventType, data);
          pollingEventsStored++;
          console.log(`[REAL-TIME-BROADCASTER] Stored polling event for user "${userId}"`);
        } catch (pollingError) {
          console.error(`[REAL-TIME-BROADCASTER] Failed to store polling event for user "${userId}":`, pollingError.message);
        }
      }

      console.log(`[REAL-TIME-BROADCASTER] Polling events stored: ${pollingEventsStored}/${roomUsers.size}`);
    } else {
      console.warn(`[REAL-TIME-BROADCASTER] ⚠️  No room users found for polling fallback storage`);
      console.warn(`[REAL-TIME-BROADCASTER] Room "${roomName}" users:`, roomUsers ? Array.from(roomUsers) : 'null');
    }

    console.log(`[REAL-TIME-BROADCASTER] Broadcast complete for event "${eventType}" in room "${roomName}"`);
    console.log('='.repeat(80));
  }

  // Send to specific user
  sendToUser(userId, eventType, data) {
    const success = this.sseManager.sendToUser(userId, eventType, data);

    // Always store for polling fallback
    this.pollingManager.addEvent(userId, eventType, data);

    return success;
  }

  // Get SSE connection for user
  addSSEConnection(userId, roomName, res) {
    return this.sseManager.addConnection(userId, roomName, res);
  }

  // Get polling events
  getPollingEvents(userId, since = 0) {
    return this.pollingManager.getEvents(userId, since);
  }

  // Get connection stats
  getStats() {
    return this.sseManager.getStats();
  }
}

// Global broadcaster and session manager instances - initialized in startServer
var realTimeBroadcaster = null;
var sseSessionManager = null;

function startServer() {
  // Initialize the global broadcaster inside startServer to ensure single instance
  if (!realTimeBroadcaster) {
    console.log('[Server] Initializing single RealTimeEventBroadcaster instance...');
    realTimeBroadcaster = new RealTimeEventBroadcaster();
    console.log('[Server] RealTimeEventBroadcaster initialized successfully');
  }

  // SSE Session Manager will be initialized after DropboxManager

  const connectionPools = new Map();
  const poolConfigs = {
    max: 10,
    min: 2,
    acquireTimeoutMillis: 8000,
    createTimeoutMillis: 8000,
    destroyTimeoutMillis: 3000,
    idleTimeoutMillis: 20000,
    reapIntervalMillis: 1000,
    createRetryIntervalMillis: 200,
    propagateCreateError: false
  };


  // NOTE: Using global SSE classes defined at the top of the file
  // All SSE functionality is handled by the global realTimeBroadcaster instance

  // =============================================================================
  // ENHANCED SESSION MANAGER WITH REAL-TIME PROGRESS TRACKING
  // =============================================================================
  class SessionManager {
    constructor(dropboxManager, storeChunkMetadata) {
      this.roomSessions = new Map();       // roomName -> Set of userIds
      this.userToRoom = new Map();         // userId -> roomName
      this.userFiles = new Map();          // userId -> Set of fileIds
      this.activeUploads = new Map();      // fileId -> upload data
      this.waitingChunks = new Map();      // fileId -> array of waiting chunks
      this.byteProgress = new Map();       // fileId -> chunkIndex -> progress
      this.dropboxManager = dropboxManager; // Store reference to DropboxManager
      this.storeChunkMetadata = storeChunkMetadata; // Store reference to storeChunkMetadata function
      this.startPeriodicCleanup();
    }

    addUserToRoom(userId, roomName) {
      if (!this.roomSessions.has(roomName)) {
        this.roomSessions.set(roomName, new Set());
      }
      this.roomSessions.get(roomName).add(userId);
      this.userToRoom.set(userId, roomName);
      console.log(`[Session] User ${userId} joined room ${roomName}`);
    }

    removeUserFromRoom(userId) {
      const roomName = this.userToRoom.get(userId);
      if (roomName && this.roomSessions.has(roomName)) {
        this.roomSessions.get(roomName).delete(userId);
        if (this.roomSessions.get(roomName).size === 0) {
          this.roomSessions.delete(roomName);
        }
      }
      this.userToRoom.delete(userId);
      console.log(`[Session] User ${userId} left room ${roomName}`);
    }

    getRoomUsers(roomName) {
      return this.roomSessions.get(roomName) || new Set();
    }

    isRoomActive(roomName) {
      const users = this.getRoomUsers(roomName);
      return users.size > 0;
    }

    addFileToUser(userId, fileId) {
      if (!this.userFiles.has(userId)) {
        this.userFiles.set(userId, new Set());
      }
      this.userFiles.get(userId).add(fileId);
    }

    startPeriodicCleanup() {
      setInterval(() => {
        this.cleanupAbandonedUploads();
        this.cleanupOldTempFiles();
      }, 3600000); // Every hour
    }

    cleanupAbandonedUploads() {
      const now = Date.now();
      const threshold = 24 * 60 * 60 * 1000; // 24 hours

      for (const [fileId, uploadData] of this.activeUploads) {
        if (now - uploadData.lastUpdate > threshold) {
          console.log(`[Cleanup] Removing abandoned upload: ${fileId}`);
          this.activeUploads.delete(fileId);

          if (uploadData.filePath && fs.existsSync(uploadData.filePath)) {
            try {
              fs.unlinkSync(uploadData.filePath);
            } catch (error) {
              console.warn(`[Cleanup] Error deleting file:`, error.message);
            }
          }
        }
      }
    }

    cleanupOldTempFiles() {
      try {
        if (!fs.existsSync(TEMP_UPLOAD_DIR)) return;

        const files = fs.readdirSync(TEMP_UPLOAD_DIR);
        const now = Date.now();
        const maxAge = 24 * 60 * 60 * 1000; // 24 hours

        for (const file of files) {
          const filePath = path.join(TEMP_UPLOAD_DIR, file);
          try {
            const stats = fs.statSync(filePath);
            if (now - stats.mtime.getTime() > maxAge) {
              fs.unlinkSync(filePath);
              console.log(`[Cleanup] Deleted old temp file: ${file}`);
            }
          } catch (error) {
            console.warn(`[Cleanup] Error processing ${filePath}:`, error.message);
          }
        }
      } catch (error) {
        console.warn(`[Cleanup] Error during cleanup:`, error.message);
      }
    }

    async processChunk(chunkData) {
      const { fileId, chunkIndex, userId } = chunkData;

      try {
        if (!fs.existsSync(chunkData.filePath)) {
          throw new Error(`Temp file missing: ${chunkData.filePath}`);
        }

        const stats = fs.statSync(chunkData.filePath);
        if (chunkData.startByte >= stats.size) {
          throw new Error(`Invalid chunk bounds`);
        }

        const eligibleAccounts = await this.dropboxManager.getEligibleAccounts(chunkData.chunkSize);

        if (eligibleAccounts.length === 0) {
          console.log(`[Upload] No eligible accounts, queuing chunk ${chunkIndex}`);
          this.addToWaitingChunks(fileId, chunkData);
          return;
        }

        let uploadResult = null;
        let lastError = null;

        for (const account of eligibleAccounts) {
          try {
            const onProgress = (progressInfo) => {
              this.updateByteProgress(fileId, chunkIndex, progressInfo, userId);
            };

            uploadResult = await this.dropboxManager.uploadChunkStream(account, chunkData, onProgress);
            break;
          } catch (error) {
            lastError = error;
            console.warn(`[Upload] Chunk ${chunkIndex} failed:`, error.message);
            continue;
          }
        }

        if (!uploadResult) {
          throw lastError || new Error(`All accounts failed for chunk ${chunkIndex}`);
        }

        await this.storeChunkMetadata({
          fileId,
          fileName: chunkData.fileName,
          chunkIndex,
          totalChunks: chunkData.totalChunks,
          roomName: chunkData.roomName,
          url: uploadResult.url,
          size: chunkData.chunkSize,
          checksum: uploadResult.checksum || "N/A"
        });

        await this.updateProgress(fileId, chunkIndex, chunkData.chunkSize, userId);

        const uploadData = this.activeUploads.get(fileId);
        if (uploadData && uploadData.uploadedChunks >= uploadData.totalChunks) {
          try {
            if (fs.existsSync(chunkData.filePath)) {
              fs.unlinkSync(chunkData.filePath);
              console.log(`[Cleanup] Deleted completed file: ${chunkData.filePath}`);
            }
          } catch (cleanupError) {
            console.error(`[Cleanup] Error deleting file:`, cleanupError.message);
          }
        }

      } catch (error) {
        console.error(`[Upload] Chunk error:`, error.message);
        await this.handleChunkError(fileId, chunkIndex, error, userId);
      }
    }

    updateByteProgress(fileId, chunkIndex, progressInfo, userId) {
      const session = this.activeUploads.get(fileId);
      if (!session) return;

      const uploadData = session;

      // Initialize file-wide byte counter
      uploadData.fileUploadedBytes = uploadData.fileUploadedBytes || 0;
      uploadData.totalSize = uploadData.totalSize || uploadData.fileSize;

      const delta = progressInfo.uploadedBytesSinceLast || 0;
      uploadData.fileUploadedBytes += delta;

      const totalProgress = (uploadData.fileUploadedBytes / uploadData.totalSize) * 100;
      const uploadedMB = (uploadData.fileUploadedBytes / (1024 * 1024)).toFixed(2);
      const speed = progressInfo.speed || 0;
      const eta = progressInfo.eta || null;

      // Send progress via SSE broadcast
      realTimeBroadcaster.broadcast(uploadData.roomName, "byte-progress", {
        fileId,
        uploadedBytes: uploadData.fileUploadedBytes,
        totalProgress,
        uploadedMB,
        speed,
        eta,
        userId
      });

      console.log(`[SSE] File-level byte-progress: fileId=${fileId}, uploaded=${uploadData.fileUploadedBytes}B, progress=${totalProgress.toFixed(1)}%`);
    }

    addToWaitingChunks(fileId, chunkData) {
      if (!this.waitingChunks.has(fileId)) {
        this.waitingChunks.set(fileId, []);
      }
      this.waitingChunks.get(fileId).push(chunkData);

      setTimeout(() => {
        this.processWaitingChunks(fileId);
      }, 30000); // Retry after 30 seconds
    }

    async processWaitingChunks(fileId) {
      const chunks = this.waitingChunks.get(fileId);
      if (!chunks || chunks.length === 0) {
        this.waitingChunks.delete(fileId);
        return;
      }

      const chunk = chunks[0];
      const eligibleAccounts = await this.dropboxManager.getEligibleAccounts(chunk.chunkSize);

      if (eligibleAccounts.length > 0) {
        this.waitingChunks.get(fileId).shift();
        if (this.waitingChunks.get(fileId).length === 0) {
          this.waitingChunks.delete(fileId);
        }
        this.processChunk(chunk);
      } else {
        setTimeout(() => {
          this.processWaitingChunks(fileId);
        }, 30000);
      }
    }

    async updateProgress(fileId, chunkIndex, chunkSize, userId, progressData = null) {
      try {
        const uploadData = this.activeUploads.get(fileId);
        if (!uploadData) {
          console.warn(`[Progress] No upload data for ${fileId}`);
          return;
        }

        uploadData.uploadedChunks = (uploadData.uploadedChunks || 0) + 1;
        uploadData.uploadedBytes = (uploadData.uploadedBytes || 0) + chunkSize;
        uploadData.progress = (uploadData.uploadedBytes / uploadData.totalSize) * 100;
        uploadData.lastUpdate = Date.now();

        if (progressData) {
          uploadData.currentChunkProgress = progressData.chunkProgress;
          uploadData.currentChunkIndex = chunkIndex;
        }

        if (userId) {
          const progressUpdate = {
            fileId,
            fileName: uploadData.fileName,
            progress: uploadData.progress,
            uploadedMB: (uploadData.uploadedBytes / (1024 * 1024)).toFixed(2),
            totalSizeMB: (uploadData.totalSize / (1024 * 1024)).toFixed(2),
            uploadedChunks: uploadData.uploadedChunks,
            totalChunks: uploadData.totalChunks,
            chunkIndex,
            currentChunkProgress: progressData?.chunkProgress || 0,
            speed: progressData?.speed || 0,
            estimatedTimeRemaining: this.calculateETA(uploadData),
            status: 'uploading',
            userId
          };

          // Send progress via SSE broadcast
          realTimeBroadcaster.broadcast(uploadData.roomName, "upload-progress", progressUpdate);
        }

        if (uploadData.uploadedChunks >= uploadData.totalChunks) {
          await this.markFileComplete(fileId, userId);
        }
      } catch (error) {
        console.error(`[Progress] Error updating progress:`, error.message);
      }
    }

    calculateETA(progressData) {
      const elapsed = Date.now() - progressData.startTime;
      const progress = progressData.progress / 100;

      if (progress > 0.01) {
        const totalEstimated = elapsed / progress;
        const remaining = totalEstimated - elapsed;
        return Math.max(0, Math.round(remaining / 1000));
      }

      return null;
    }

    async markFileComplete(fileId, userId) {
      try {
        const uploadData = this.activeUploads.get(fileId);
        if (!uploadData) {
          console.warn(`[Progress] No upload data for completion: ${fileId}`);
          return;
        }

        uploadData.status = 'completed';
        uploadData.completedAt = Date.now();
        uploadData.progress = 100;

        const totalTime = (uploadData.completedAt - uploadData.startTime) / 1000;

        if (userId) {
          realTimeBroadcaster.broadcast(uploadData.roomName, "file-upload-complete", {
            fileId,
            fileName: uploadData.fileName,
            message: "File upload completed successfully",
            totalTime: totalTime,
            totalSize: uploadData.totalSize,
            totalChunks: uploadData.totalChunks,
            userId
          });
        }

        if (uploadData.roomName) {
          realTimeBroadcaster.broadcast(uploadData.roomName, "new-content", {
            type: "file",
            fileId,
            fileName: uploadData.fileName,
            message: "New file uploaded",
            fileSize: uploadData.totalSize
          });
        }

        console.log(`[Upload] File ${fileId} completed in ${totalTime.toFixed(2)}s`);
      } catch (error) {
        console.error(`[Progress] Error marking complete:`, error.message);
      }
    }

    async handleChunkError(fileId, chunkIndex, error, userId) {
      try {
        const uploadData = this.activeUploads.get(fileId);
        if (!uploadData) {
          console.warn(`[Progress] No upload data for error: ${fileId}`);
          return;
        }

        if (!uploadData.errors) uploadData.errors = [];
        uploadData.errors.push({
          chunkIndex,
          error: error.message,
          timestamp: Date.now()
        });

        if (uploadData.errors.length > Math.ceil(uploadData.totalChunks * 0.1)) {
          uploadData.status = 'failed';
          uploadData.failedAt = Date.now();

          if (userId) {
            realTimeBroadcaster.broadcast(uploadData.roomName, "file-upload-failed", {
              fileId,
              fileName: uploadData.fileName,
              error: `Too many chunk failures: ${uploadData.errors.length}`,
              totalErrors: uploadData.errors.length,
              userId
            });
          }
        }

        if (userId) {
          realTimeBroadcaster.broadcast(uploadData.roomName, "upload-error", {
            fileId,
            chunkIndex,
            error: error.message,
            totalErrors: uploadData.errors.length,
            maxErrors: Math.ceil(uploadData.totalChunks * 0.1),
            userId
          });
        }

        console.warn(`[Progress] Chunk error for ${fileId}: ${error.message}`);
      } catch (error) {
        console.error(`[Progress] Error handling error:`, error.message);
      }
    }
  }

  // =============================================================================
  // DROPBOX MANAGER WITH REAL-TIME PROGRESS
  // =============================================================================
  class DropboxManager {
    constructor() {
      this.accountLocks = new Map();
    }

    async getEligibleAccounts(requiredSpace = MAX_CHUNK_SIZE) {
      try {
        const accounts = await this.fetchDropboxAccounts();
        const eligible = [];

        for (const acc of accounts) {
          const used = parseFloat(acc.fields.storage || "0");
          const free = DROPBOX_ACCOUNT_MAX_CAPACITY - used;
          const isLocked = await this.isAccountLocked(acc.id);

          const nearCapacity = used > DROPBOX_ROTATION_THRESHOLD;
          if (free >= requiredSpace && !isLocked && !nearCapacity) {
            eligible.push({
              ...acc,
              freeSpace: free,
              priority: free
            });
          }
        }

        eligible.sort((a, b) => b.freeSpace - a.freeSpace);
        return eligible;
      } catch (error) {
        console.error("[Dropbox] Error fetching accounts:", error.message);
        return [];
      }
    }

    async fetchDropboxAccounts() {
      const basesResponse = await fetch("https://api.airtable.com/v0/meta/bases", {
        headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
        timeout: 10000
      });

      if (!basesResponse.ok) {
        throw new Error(`Dropbox bases fetch failed: ${basesResponse.status}`);
      }

      const basesData = await basesResponse.json();
      const dropBases = basesData.bases.filter(b => b.name.toLowerCase() === "drop");

      let accounts = [];
      for (const base of dropBases) {
        const url = `https://api.airtable.com/v0/${base.id}/Table%201`;
        try {
          const response = await fetch(url, {
            headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
            timeout: 10000
          });

          if (response.ok) {
            const data = await response.json();
            accounts = accounts.concat(
              data.records.map(record => ({ ...record, baseId: base.id }))
            );
          }
        } catch (error) {
          console.warn(`[Dropbox] Error fetching base ${base.id}:`, error.message);
        }
      }

      return accounts;
    }

    async isAccountLocked(accountId) {
      return this.accountLocks.has(accountId);
    }

    async acquireLock(account, lockId, lockOwner) {
      if (this.accountLocks.has(account.id)) {
        return false;
      }

      this.accountLocks.set(account.id, lockId);
      console.log(`[Dropbox] Lock acquired for account ${account.id}`);

      try {
        await this.updateAccountLockStatus(account, {
          "upload lock": lockId,
          "lock timestamp": new Date().toISOString(),
          "lock owner": lockOwner,
          "lock expiry": new Date(Date.now() + 3600000).toISOString(),
          "availability": "0"
        });
      } catch (error) {
        console.error(`[Dropbox] Error updating lock status:`, error.message);
        this.accountLocks.delete(account.id);
        return false;
      }

      return true;
    }

    async releaseLock(account, lockId) {
      if (this.accountLocks.get(account.id) !== lockId) {
        console.warn(`[Dropbox] Lock mismatch for account ${account.id}`);
        return false;
      }

      this.accountLocks.delete(account.id);
      console.log(`[Dropbox] Lock released for account ${account.id}`);

      try {
        await this.updateAccountLockStatus(account, {
          "upload lock": null,
          "lock timestamp": null,
          "lock owner": null,
          "lock expiry": null,
          "availability": "1"
        });
      } catch (error) {
        console.error(`[Dropbox] Error updating lock status:`, error.message);
      }

      return true;
    }

    async updateAccountLockStatus(account, fields) {
      const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
      const response = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ fields }),
        timeout: 15000
      });

      if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`Failed to update lock status: ${response.status} - ${errorBody}`);
      }
    }

    async uploadChunkStream(account, chunkData, onProgress) {
      const { filePath, chunkSize, startByte, fileName, chunkIndex, fileSize, fileId, userId } = chunkData;
      const lockId = `${fileId}_${chunkIndex}_${Date.now()}`;

      if (!fs.existsSync(filePath)) {
        throw new Error(`Temp file missing: ${filePath}`);
      }

      const fileStats = fs.statSync(filePath);
      if (startByte >= fileStats.size) {
        throw new Error(`Invalid byte range: ${startByte} >= ${fileStats.size}`);
      }

      const lockAcquired = await this.acquireLock(account, lockId, fileId);
      if (!lockAcquired) {
        throw new Error(`Failed to acquire lock for account ${account.id}`);
      }

      try {
        let accessToken = account.fields.accesstoken;
        if (!accessToken || this.isTokenExpired(accessToken)) {
          console.log(`[Dropbox] Refreshing token for account ${account.id}`);
          accessToken = await this.refreshAccessToken(account);
        }

        let uploadResult = null;
        let lastError = null;

        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            const dropboxPath = `/infinityshare/${fileName}_chunk_${chunkIndex}_${uuidv4()}`;

            const readStream = fs.createReadStream(filePath, {
              start: startByte,
              end: startByte + chunkSize - 1,
              highWaterMark: 1024 // 1KB chunks
            });

            const progressEmitter = new ProgressEmitter(
              fileId,
              chunkIndex,
              userId,
              chunkSize,
              startByte,
              fileSize,
              onProgress
            );

            const uploadPromise = fetch("https://content.dropboxapi.com/2/files/upload", {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${accessToken}`,
                "Content-Type": "application/octet-stream",
                "Dropbox-API-Arg": JSON.stringify({
                  path: dropboxPath,
                  mode: "add",
                  autorename: true,
                  mute: false
                }),
                "Content-Length": chunkSize.toString()
              },
              body: readStream.pipe(progressEmitter),
              timeout: UPLOAD_TIMEOUT
            });

            const response = await Promise.race([
              uploadPromise,
              new Promise((_, reject) =>
                setTimeout(() => reject(new Error('Upload timeout')), UPLOAD_TIMEOUT)
              )
            ]);

            if (!response.ok) {
              const errorText = await response.text();
              if (response.status === 401) {
                accessToken = await this.refreshAccessToken(account);
                account.fields.accesstoken = accessToken;
                continue;
              }
              throw new Error(`Dropbox upload failed: ${errorText}`);
            }

            const permanentUrl = await this.createPermanentLink(accessToken, dropboxPath);
            await this.updateStorageUsage(account, chunkSize);

            uploadResult = {
              url: permanentUrl,
              path: dropboxPath,
              size: chunkSize,
              checksum: response.headers.get('content-md5') || "N/A"
            };
            break;

          } catch (error) {
            lastError = error;
            if (error.message.includes('401') && attempt === 0) {
              try {
                accessToken = await this.refreshAccessToken(account);
                account.fields.accesstoken = accessToken;
              } catch (refreshError) {
                break;
              }
            } else {
              break;
            }
          }
        }

        if (!uploadResult) {
          throw lastError || new Error(`All attempts failed for account ${account.id}`);
        }

        return uploadResult;

      } finally {
        await this.releaseLock(account, lockId);
      }
    }

    async createPermanentLink(accessToken, dropboxPath) {
      try {
        const response = await fetch("https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            path: dropboxPath,
            settings: { requested_visibility: "public" }
          }),
          timeout: 30000
        });

        let result;
        if (!response.ok) {
          const errorData = await response.json();
          if (errorData.error_summary?.includes("shared_link_already_exists")) {
            const listResponse = await fetch("https://api.dropboxapi.com/2/sharing/list_shared_links", {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${accessToken}`,
                "Content-Type": "application/json"
              },
              body: JSON.stringify({ path: dropboxPath, direct_only: true }),
              timeout: 30000
            });

            const listData = await listResponse.json();
            result = listData.links[0];
          } else {
            throw new Error(`Failed to create shared link: ${errorData.error_summary}`);
          }
        } else {
          result = await response.json();
        }

        let directUrl = result.url;
        if (directUrl.includes("dl=0")) {
          directUrl = directUrl.replace("dl=0", "dl=1");
        } else if (!directUrl.includes("dl=1")) {
          const separator = directUrl.includes("?") ? "&" : "?";
          directUrl += `${separator}dl=1`;
        }

        return directUrl;
      } catch (error) {
        console.error("[Dropbox] Error creating permanent link:", error);
        throw error;
      }
    }

    async refreshAccessToken(account) {
      const params = new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: account.fields.refreshtoken,
        client_id: account.fields.appkey,
        client_secret: account.fields.appsecret
      });

      const response = await fetch('https://api.dropboxapi.com/oauth2/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params,
        timeout: 30000
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(`Token refresh failed: ${error.error_description || error.error}`);
      }

      const tokenData = await response.json();
      await this.updateAccountToken(account, tokenData.access_token);
      return tokenData.access_token;
    }

    async updateAccountToken(account, newToken) {
      const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
      const response = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          fields: { accesstoken: newToken }
        }),
        timeout: 15000
      });

      if (!response.ok) {
        throw new Error(`Failed to update token: ${response.status}`);
      }

      account.fields.accesstoken = newToken;
    }

    async updateStorageUsage(account, additionalBytes) {
      const currentStorage = parseFloat(account.fields.storage || "0");
      const newStorage = currentStorage + additionalBytes;

      const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
      const response = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          fields: { storage: newStorage.toString() }
        }),
        timeout: 15000
      });

      if (!response.ok) {
        throw new Error(`Failed to update storage: ${response.status}`);
      }

      account.fields.storage = newStorage.toString();
    }

    isTokenExpired(token) {
      return false;
    }
  }

  // =============================================================================
  // PROGRESS EMITTER TRANSFORM STREAM
  // =============================================================================
  class ProgressEmitter extends Transform {
    constructor(fileId, chunkIndex, userId, chunkSize, startByte, fileSize, onProgress) {
      super();
      this.fileId = fileId;
      this.chunkIndex = chunkIndex;
      this.userId = userId;
      this.chunkSize = chunkSize;
      this.startByte = startByte;
      this.fileSize = fileSize;
      this.onProgress = onProgress;
      this.uploadedBytes = 0;
      this.lastEmit = 0;
      this.startTime = Date.now();
      this.lastBytes = 0;
    }
    _transform(chunk, encoding, callback) {
      this.uploadedBytes += chunk.length;
      const now = Date.now();
      const absolutePosition = this.startByte + this.uploadedBytes;
      const chunkProgress = (this.uploadedBytes / this.chunkSize) * 100;
      const totalProgress = (absolutePosition / this.fileSize) * 100;
      const timeDelta = (now - this.startTime) / 1000;
      const bytesDelta = this.uploadedBytes - this.lastBytes;
      const speed = timeDelta > 0 ? bytesDelta / timeDelta : 0;
      const eta = totalProgress > 0 ? ((timeDelta / (totalProgress / 100)) - timeDelta) : null;

      // Emit every 1KB or every 10ms
      if (this.uploadedBytes - this.lastBytes >= 1024 || now - this.lastEmit >= 10) {
        const delta = this.uploadedBytes - this.lastBytes;

        console.log(`[Upload KB] Emitting byte-progress for file ${this.fileId}, chunk ${this.chunkIndex}: ${this.uploadedBytes} bytes uploaded`);

        if (this.onProgress) {
          this.onProgress({
            uploadedBytes: this.uploadedBytes,
            uploadedBytesSinceLast: delta,
            absolutePosition,
            chunkProgress,
            totalProgress,
            speed,
            eta
          });
        }

        this.lastEmit = now;
        this.lastBytes = this.uploadedBytes;
      }

      this.push(chunk);
      callback();
    }



  }
  // =============================================================================
  // DATABASE CONNECTION MANAGEMENT (UNCHANGED)
  // =============================================================================
  async function getOptimizedConnections() {
    try {
      console.log("[Neon] Fetching fresh connections from Airtable");
      const response = await fetch("https://api.airtable.com/v0/meta/bases", {
        headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
        timeout: 10000
      });

      if (!response.ok) {
        throw new Error(`Airtable fetch failed: ${response.status} ${response.statusText}`);
      }

      const basesData = await response.json();
      const targetBases = basesData.bases.filter(b => b.name === "Base1");
      console.log(`[Airtable] Found ${targetBases.length} target bases`);

      let allRecords = [];
      for (const base of targetBases) {
        const url = `https://api.airtable.com/v0/${base.id}/Table%201`;
        try {
          const connectionsResponse = await fetch(url, {
            headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
            timeout: 10000
          });

          if (connectionsResponse.ok) {
            const connectionsData = await connectionsResponse.json();
            allRecords = allRecords.concat(
              connectionsData.records.map(record => ({ ...record, baseId: base.id }))
            );
            console.log(`[Airtable] Loaded ${connectionsData.records.length} records from base ${base.id}`);
          } else {
            console.warn(`[Airtable] Failed to fetch from base ${base.id}: ${connectionsResponse.status}`);
          }
        } catch (error) {
          console.warn(`[Airtable] Error fetching base ${base.id}:`, error.message);
        }
      }

      console.log(`[Airtable] Total records fetched: ${allRecords.length}`);
      return { records: allRecords };
    } catch (error) {
      console.error("[Neon] Error fetching connections:", error.message);
      throw error;
    }
  }

  function getConnectionPool(record) {
    const poolKey = record.id;

    if (!connectionPools.has(poolKey)) {
      console.log(`[Pool] Creating new connection pool for ${poolKey}`);
      const pool = new Pool({
        connectionString: record.fields.connectionstring,
        ssl: { rejectUnauthorized: false },
        ...poolConfigs
      });

      pool.on('error', (err) => console.error(`[Pool ${poolKey}] Error:`, err.message));
      pool.on('connect', () => console.log(`[Pool ${poolKey}] New client connected`));
      pool.on('remove', () => console.log(`[Pool ${poolKey}] Client removed`));

      connectionPools.set(poolKey, pool);
    }

    return connectionPools.get(poolKey);
  }

  async function getBestNeonConnection() {
    const connections = await getOptimizedConnections();
    let bestConnection = null;
    let maxSpace = 0;

    for (const record of connections.records) {
      try {
        const pool = getConnectionPool(record);
        const client = await pool.connect();
        const dbSize = await getDatabaseSizeMB(client);
        const freeSpace = MAX_DB_CAPACITY_MB - dbSize;

        if (freeSpace > maxSpace && freeSpace > (MAX_DB_CAPACITY_MB - DB_ROTATION_THRESHOLD)) {
          maxSpace = freeSpace;
          bestConnection = { record, pool, freeSpace };
        }
        client.release();
      } catch (error) {
        console.warn(`[Connection] Error checking DB ${record.id}:`, error.message);
      }
    }

    if (!bestConnection) throw new Error('No eligible Neon connections available');
    return bestConnection;
  }

  async function getDatabaseSizeMB(client) {
    try {
      const result = await client.query("SELECT pg_database_size(current_database()) AS size");
      return result.rows[0].size / (1024 * 1024);
    } catch (error) {
      console.error('[DB] Error getting database size:', error.message);
      return 0;
    }
  }

  async function ensureChunksTables(client) {
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS message_chunks (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) NOT NULL,
          message_id UUID NOT NULL,
          chunk_index INT NOT NULL,
          total_chunks INT NOT NULL,
          content TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          CONSTRAINT unique_message_chunk UNIQUE (message_id, chunk_index)
        );
        
        CREATE TABLE IF NOT EXISTS file_chunks (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) NOT NULL,
          file_id UUID NOT NULL,
          file_name VARCHAR(255) NOT NULL,
          chunk_index INT NOT NULL,
          total_chunks INT NOT NULL,
          checksum TEXT NOT NULL,
          filechunk_url TEXT NOT NULL,
          chunk_size BIGINT DEFAULT 0,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          CONSTRAINT unique_file_chunk UNIQUE (file_id, chunk_index)
        );
        
        CREATE TABLE IF NOT EXISTS rooms (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) UNIQUE NOT NULL,
          room_password VARCHAR(255) NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_file_chunks_room_file ON file_chunks(room_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_message_chunks_room_msg ON message_chunks(room_name, message_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_created_at ON file_chunks(created_at);
        CREATE INDEX IF NOT EXISTS idx_file_chunks_room_created ON file_chunks(room_name, created_at DESC);
      `);

      const checkResult = await client.query(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'file_chunks' AND column_name = 'chunk_size'
      `);

      if (checkResult.rows.length === 0) {
        console.log('[DB] Adding chunk_size column to file_chunks table');
        await client.query('ALTER TABLE file_chunks ADD COLUMN chunk_size BIGINT DEFAULT 0');
      }
    } catch (error) {
      console.error('[DB] Error ensuring tables exist:', error.message);
      throw error;
    }
  }

  async function storeChunkMetadata(chunkData) {
    console.log(`[DB] Storing metadata for chunk ${chunkData.chunkIndex} of file ${chunkData.fileId}`);

    try {
      const { record, pool, freeSpace } = await getBestNeonConnection();
      console.log(`[DB] Selected connection ${record.id} with ${freeSpace.toFixed(2)}MB free`);

      const client = await pool.connect();

      try {
        await ensureChunksTables(client);
        const result = await client.query(`
          INSERT INTO file_chunks (
            room_name, file_id, file_name, chunk_index, total_chunks, 
            checksum, filechunk_url, chunk_size, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
          ON CONFLICT (file_id, chunk_index) DO UPDATE SET
            filechunk_url = EXCLUDED.filechunk_url,
            chunk_size = EXCLUDED.chunk_size,
            created_at = NOW()
          RETURNING id
        `, [
          chunkData.roomName,
          chunkData.fileId,
          chunkData.fileName,
          chunkData.chunkIndex,
          chunkData.totalChunks,
          chunkData.checksum,
          chunkData.url,
          chunkData.size
        ]);

        console.log(`[DB] Stored metadata for chunk ${chunkData.chunkIndex} in DB ${record.id} (record ID: ${result.rows[0].id})`);

      } finally {
        client.release();
      }
    } catch (error) {
      console.error(`[DB] Error storing chunk metadata:`, error.message);
      throw error;
    }
  }

  // =============================================================================
  // MESSAGE PROCESSING (UNCHANGED)
  // =============================================================================
  class MessageProcessor {
    static async processMessage(message, roomName, userId) {
      const messageId = uuidv4();
      const chunks = this.splitMessage(message, MESSAGE_CHUNK_SIZE);
      console.log(`[Message] Processing message ${messageId} with ${chunks.length} chunks`);

      try {
        const connections = await getOptimizedConnections();
        const tasks = chunks.map((chunk, index) =>
          this.storeMessageChunk({
            messageId,
            roomName,
            chunkIndex: index,
            totalChunks: chunks.length,
            content: chunk,
            connections: connections.records
          })
        );

        await Promise.all(tasks);

        realTimeBroadcaster.broadcast(roomName, "new-content", {
          type: "message",
          messageId,
          preview: message.substring(0, 100) + (message.length > 100 ? '...' : ''),
          message: "New message posted",
          timestamp: new Date().toISOString()
        });

        console.log(`[Message] Message ${messageId} processed successfully`);
        return { messageId, chunks: chunks.length };
      } catch (error) {
        console.error(`[Message] Error processing message ${messageId}:`, error.message);
        throw error;
      }
    }

    static splitMessage(message, chunkSize) {
      const chunks = [];
      for (let i = 0; i < message.length; i += chunkSize) {
        chunks.push(message.substring(i, i + chunkSize));
      }
      return chunks;
    }

    static async storeMessageChunk({ messageId, roomName, chunkIndex, totalChunks, content, connections }) {
      const selectedConnection = connections[chunkIndex % connections.length];
      const pool = getConnectionPool(selectedConnection);
      const client = await pool.connect();

      try {
        await ensureChunksTables(client);
        await client.query(`
          INSERT INTO message_chunks (room_name, message_id, chunk_index, total_chunks, content)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (message_id, chunk_index) DO NOTHING
        `, [roomName, messageId, chunkIndex, totalChunks, content]);
      } finally {
        client.release();
      }
    }
  }

  // =============================================================================
  // CONTENT RETRIEVAL (UNCHANGED)
  // =============================================================================
  class ContentRetriever {
    static async getRoomContent(roomName, limit = 50, offset = 0) {
      try {
        const connections = await getOptimizedConnections();
        const [messages, files] = await Promise.all([
          this.getMessages(roomName, connections.records, limit, offset),
          this.getFiles(roomName, connections.records, limit, offset)
        ]);

        const content = [...messages, ...files]
          .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
          .slice(0, limit);

        return { content, total: content.length };
      } catch (error) {
        console.error(`[Content] Error retrieving room content:`, error.message);
        throw error;
      }
    }

    static async getMessages(roomName, connections, limit, offset) {
      const messageResults = await Promise.all(
        connections.map(async (record) => {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            await ensureChunksTables(client);
            const result = await client.query(`
              SELECT message_id, chunk_index, total_chunks, content, created_at
              FROM message_chunks 
              WHERE room_name = $1
              ORDER BY created_at DESC
              LIMIT $2 OFFSET $3
            `, [roomName, limit * 2, offset]);
            return result.rows;
          } catch (error) {
            console.warn(`[Content] Error fetching messages from DB ${record.id}:`, error.message);
            return [];
          } finally {
            client.release();
          }
        })
      );

      const allChunks = messageResults.flat();
      const messageMap = new Map();

      allChunks.forEach(chunk => {
        if (!messageMap.has(chunk.message_id)) {
          messageMap.set(chunk.message_id, {
            chunks: [],
            created_at: chunk.created_at
          });
        }
        messageMap.get(chunk.message_id).chunks.push(chunk);
      });

      const messages = [];
      for (const [messageId, data] of messageMap) {
        const expectedChunks = data.chunks[0]?.total_chunks || 0;
        if (data.chunks.length === expectedChunks) {
          data.chunks.sort((a, b) => a.chunk_index - b.chunk_index);
          const fullMessage = data.chunks.map(c => c.content).join('');
          messages.push({
            type: 'message',
            message_id: messageId,
            message: fullMessage,
            created_at: data.created_at,
            chunks: data.chunks.length
          });
        }
      }

      return messages.slice(0, limit);
    }

    static async getFiles(roomName, connections, limit, offset) {
      const fileResults = await Promise.all(
        connections.map(async (record) => {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            await ensureChunksTables(client);
            const result = await client.query(`
              SELECT DISTINCT file_id, file_name, 
                     MIN(created_at) as created_at,
                     COUNT(*) as available_chunks,
                     MAX(total_chunks) as total_chunks,
                     SUM(chunk_size) as file_size
              FROM file_chunks 
              WHERE room_name = $1
              GROUP BY file_id, file_name
              ORDER BY created_at DESC
              LIMIT $2 OFFSET $3
            `, [roomName, limit, offset]);
            return result.rows;
          } catch (error) {
            console.warn(`[Content] Error fetching files from DB ${record.id}:`, error.message);
            return [];
          } finally {
            client.release();
          }
        })
      );

      const allFiles = fileResults.flat();
      const uniqueFiles = new Map();

      allFiles.forEach(file => {
        const existingFile = uniqueFiles.get(file.file_id);
        if (!existingFile || new Date(file.created_at) < new Date(existingFile.created_at)) {
          uniqueFiles.set(file.file_id, {
            ...file,
            isComplete: parseInt(file.available_chunks) === parseInt(file.total_chunks)
          });
        }
      });

      return Array.from(uniqueFiles.values()).map(file => ({
        type: 'file',
        file_id: file.file_id,
        file_name: file.file_name,
        created_at: file.created_at,
        file_size: parseInt(file.file_size || 0),
        available_chunks: parseInt(file.available_chunks),
        total_chunks: parseInt(file.total_chunks),
        is_complete: file.isComplete,
        progress: file.isComplete ? 100 : (parseInt(file.available_chunks) / parseInt(file.total_chunks)) * 100
      })).slice(0, limit);
    }
  }

  // =============================================================================
  // FILE DOWNLOAD (UNCHANGED)
  // =============================================================================
  class FileDownloader {
    static async streamFileChunk(roomName, fileId, chunkIndex, res) {
      try {
        console.log(`[Download] Streaming chunk ${chunkIndex} for file ${fileId}`);

        const connections = await getOptimizedConnections();
        let chunkUrl = null;
        let chunkSize = 0;

        for (const record of connections.records) {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            const result = await client.query(`
              SELECT filechunk_url, chunk_size FROM file_chunks 
              WHERE room_name = $1 AND file_id = $2 AND chunk_index = $3
              LIMIT 1
            `, [roomName, fileId, parseInt(chunkIndex)]);

            if (result.rows.length > 0) {
              chunkUrl = result.rows[0].filechunk_url;
              chunkSize = parseInt(result.rows[0].chunk_size || 0);
              break;
            }
          } finally {
            client.release();
          }
        }

        if (!chunkUrl) {
          return res.status(404).json({ error: 'Chunk not found' });
        }

        console.log(`[Download] Fetching chunk from Dropbox: ${chunkUrl}`);
        const dropboxResponse = await fetch(chunkUrl, {
          timeout: UPLOAD_TIMEOUT
        });

        if (!dropboxResponse.ok) {
          throw new Error(`Failed to fetch from Dropbox: ${dropboxResponse.status}`);
        }

        const totalSize = parseInt(dropboxResponse.headers.get('content-length') || chunkSize.toString());

        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Length', totalSize);
        res.setHeader('Cache-Control', 'public, max-age=3600');
        res.setHeader('Access-Control-Allow-Origin', '*');

        await pipeline(dropboxResponse.body, res);
        console.log(`[Download] Chunk ${chunkIndex} streamed successfully`);

      } catch (error) {
        console.error(`[Download] Error streaming chunk ${chunkIndex}:`, error.message);
        if (!res.headersSent) {
          res.status(500).json({ error: 'Download failed', details: error.message });
        }
      }
    }

    static async getFileMetadata(roomName, fileId) {
      try {
        const connections = await getOptimizedConnections();
        let metadata = null;

        for (const record of connections.records) {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            const result = await client.query(`
              SELECT COUNT(*) as available_chunks, 
                     MAX(total_chunks) as expected_chunks,
                     MAX(chunk_size) as chunk_size,
                     SUM(chunk_size) as total_size,
                     file_name,
                     MIN(created_at) as created_at,
                     MAX(created_at) as last_updated
              FROM file_chunks 
              WHERE room_name = $1 AND file_id = $2
              GROUP BY file_name
            `, [roomName, fileId]);

            if (result.rows.length > 0) {
              const row = result.rows[0];
              metadata = {
                totalChunks: parseInt(row.expected_chunks),
                availableChunks: parseInt(row.available_chunks),
                chunkSize: parseInt(row.chunk_size || 0),
                totalSize: parseInt(row.total_size || 0),
                fileName: row.file_name,
                isComplete: parseInt(row.available_chunks) === parseInt(row.expected_chunks),
                createdAt: row.created_at,
                lastUpdated: row.last_updated,
                downloadProgress: (parseInt(row.available_chunks) / parseInt(row.expected_chunks)) * 100
              };
              break;
            }
          } finally {
            client.release();
          }
        }

        return metadata;
      } catch (error) {
        console.error(`[Download] Error getting file metadata:`, error.message);
        return null;
      }
    }
  }

  // =============================================================================
  // EXPRESS & SOCKET.IO SETUP
  // =============================================================================
  const app = express();
  const server = http.createServer(app);

  // Initialize DropboxManager after class definitions
  const dropboxManager = new DropboxManager();

  // Initialize SSE Session Manager with DropboxManager and storeChunkMetadata references
  if (!sseSessionManager) {
    console.log('[Server] Initializing SSE Session Manager...');
    sseSessionManager = new SSESessionManager(dropboxManager, storeChunkMetadata);
    console.log('[Server] SSE Session Manager initialized successfully');
  }

  // Enhanced CORS configuration for SSE support
  app.use(require("cors")({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Cache-Control', 'Last-Event-ID'],
    exposedHeaders: ['Cache-Control', 'Content-Type'],
    credentials: false,
    maxAge: 86400 // 24 hours
  }));
  app.use(express.json({ limit: '10mb' }));
  app.use(express.static('public'));

  // OPTIMIZED MULTER STORAGE FOR RENDER
  const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      // Ensure temp directory exists for each request
      if (!fs.existsSync(TEMP_UPLOAD_DIR)) {
        try {
          fs.mkdirSync(TEMP_UPLOAD_DIR, { recursive: true });
          console.log(`[Upload] Created temp directory: ${TEMP_UPLOAD_DIR}`);
        } catch (error) {
          console.error(`[Upload] Error creating temp directory:`, error.message);
        }
      }
      cb(null, TEMP_UPLOAD_DIR);
    },
    filename: (req, file, cb) => {
      const sanitizedName = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
      const uniqueName = `${Date.now()}-${uuidv4()}-${sanitizedName}`;
      cb(null, uniqueName);
    }
  });

  const upload = multer({
    storage: storage,
    limits: {
      fileSize: 100 * 1024 * 1024 * 1024, // 100GB max file size
      fieldSize: 10 * 1024 * 1024,
      files: 1
    },
    fileFilter: (req, file, cb) => {
      console.log(`[Upload] Receiving file: ${file.originalname} (${TEMP_UPLOAD_DIR})`);
      if (!file.originalname || file.originalname.trim() === '') {
        return cb(new Error('Invalid filename'));
      }
      cb(null, true);
    }
  });

  // SessionManager replaced by sseSessionManager (initialized above)

  // =============================================================================
  // SOCKET.IO HANDLERS (SIMPLIFIED - REMOVED OFFLINE LOGIC)
  // =============================================================================
  app.get('/health', async (req, res) => {
    try {
      res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        worker: process.pid,
        tempDirectory: TEMP_UPLOAD_DIR,
        sessionStats: sseSessionManager.getSessionStats(),
        connectionStats: realTimeBroadcaster.getStats(),
        systemInfo: {
          platform: process.platform,
          nodeVersion: process.version,
          totalMemory: Math.round(os.totalmem() / 1024 / 1024) + 'MB',
          freeMemory: Math.round(os.freemem() / 1024 / 1024) + 'MB',
          uptime: process.uptime(),
          loadAverage: os.loadavg()
        },
        version: '5.0.0-sse-optimized'
      });
    } catch (error) {
      res.status(503).json({
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  app.post('/create-room', async (req, res) => {
    const { roomName, roomPassword } = req.body;

    if (!roomName || !roomPassword) {
      return res.status(400).json({ error: 'Missing roomName or roomPassword' });
    }

    if (roomName.length > 100 || roomPassword.length > 100) {
      return res.status(400).json({ error: 'Room name or password too long' });
    }

    try {
      console.log(`[Room] Creating room "${roomName}"`);
      const { record, pool, freeSpace } = await getBestNeonConnection();
      console.log(`[DB] Selected connection ${record.id} with ${freeSpace.toFixed(2)}MB free`);

      const client = await pool.connect();
      try {
        await ensureChunksTables(client);
        const result = await client.query(
          'SELECT room_name FROM rooms WHERE room_name = $1',
          [roomName]
        );
        if (result.rows.length > 0) {
          return res.status(400).json({ error: 'Room already exists' });
        }

        await client.query(
          'INSERT INTO rooms (room_name, room_password) VALUES ($1, $2)',
          [roomName, roomPassword]
        );
        console.log(`[Room] Created room "${roomName}" in DB ${record.id}`);
        res.json({ message: 'Room created successfully', roomName });
      } finally {
        client.release();
      }

    } catch (error) {
      console.error('[Room] Error creating room:', error.message);
      res.status(500).json({ error: 'Failed to create room', details: error.message });
    }
  });

  app.post('/join-room', async (req, res) => {
    const { roomName, roomPassword } = req.body;

    if (!roomName || !roomPassword) {
      return res.status(400).json({ error: 'Missing roomName or roomPassword' });
    }

    try {
      console.log(`[Room] Attempting to join room "${roomName}"`);
      const connections = await getOptimizedConnections();
      let roomFound = false;

      for (const record of connections.records) {
        const pool = getConnectionPool(record);
        const client = await pool.connect();
        try {
          await ensureChunksTables(client);
          const result = await client.query(
            'SELECT room_password FROM rooms WHERE room_name = $1',
            [roomName]
          );
          if (result.rows.length > 0 && result.rows[0].room_password === roomPassword) {
            roomFound = true;
            break;
          }
        } finally {
          client.release();
        }
      }

      if (roomFound) {
        console.log(`[Room] Successfully joined room "${roomName}"`);
        res.json({ message: 'Room joined successfully', roomName });
      } else {
        console.warn(`[Room] Failed to join room "${roomName}" - invalid credentials`);
        res.status(400).json({ error: 'Invalid room name or password' });
      }

    } catch (error) {
      console.error('[Room] Error joining room:', error.message);
      res.status(500).json({ error: 'Failed to join room', details: error.message });
    }
  });

  app.post('/room/:roomName/post-message', async (req, res) => {
    const { roomName } = req.params;
    const { message, userId } = req.body;

    if (!message) {
      return res.status(400).json({ error: 'Missing message' });
    }

    if (message.length > 1000000) {
      return res.status(400).json({ error: 'Message too long' });
    }

    try {
      console.log(`[Message] Posting message to room "${roomName}" (${message.length} characters)`);
      const result = await MessageProcessor.processMessage(message, roomName, userId || 'guest');
      res.json({
        message: 'Message posted successfully',
        messageId: result.messageId,
        chunks: result.chunks
      });
    } catch (error) {
      console.error('[Message] Error posting message:', error.message);
      res.status(500).json({ error: 'Failed to post message', details: error.message });
    }
  });

  app.post('/room/:roomName/upload-file', upload.single('file'), async (req, res) => {
    const { roomName } = req.params;
    const file = req.file;
    const userId = req.body.userId || 'guest';

    console.log(`[Upload] File upload request for room "${roomName}"`);
    console.log(`[Upload] Using temp directory: ${TEMP_UPLOAD_DIR}`);
    console.log(`[Upload] File info:`, file ? {
      originalname: file.originalname,
      size: file.size,
      mimetype: file.mimetype,
      path: file.path
    } : 'No file');
    console.log(`[Upload] Request body:`, req.body);
    console.log(`[Upload] SSE Broadcaster status:`, {
      initialized: realTimeBroadcaster ? 'yes' : 'no',
      connectionCount: realTimeBroadcaster ? realTimeBroadcaster.getStats().totalConnections : 0
    });

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    if (!fs.existsSync(file.path)) {
      return res.status(400).json({ error: 'File upload failed - file not found' });
    }

    const stats = fs.statSync(file.path);
    if (stats.size === 0) {
      try {
        fs.unlinkSync(file.path);
      } catch (e) { }
      return res.status(400).json({ error: 'Cannot upload empty file' });
    }

    if (stats.size > 100 * 1024 * 1024 * 1024) {
      try {
        fs.unlinkSync(file.path);
      } catch (e) { }
      return res.status(400).json({ error: 'File too large (max 100GB)' });
    }

    try {
      console.log(`[Upload] Processing file: ${file.originalname} (${(stats.size / (1024 * 1024)).toFixed(2)}MB)`);

      const fileId = uuidv4();
      const chunkSize = getOptimalChunkSize(stats.size);
      const totalChunks = Math.ceil(stats.size / chunkSize);

      // Register file with session manager
      if (userId) {
        sseSessionManager.addFileToUser(userId, fileId);
      }

      // Initialize upload data
      const uploadData = {
        fileId,
        fileName: file.originalname,
        totalSize: stats.size,
        totalChunks,
        chunkSize,
        uploadedChunks: 0,
        uploadedBytes: 0,
        progress: 0,
        status: 'processing',
        filePath: file.path,
        startTime: Date.now(),
        errors: [],
        lastUpdate: Date.now(),
        roomName,
        userId
      };

      sseSessionManager.activeUploads.set(fileId, uploadData);

      // Emit initial progress via SSE
      if (userId) {
        try {
          realTimeBroadcaster.broadcast(roomName, "file-processing-started", {
            fileId,
            fileName: uploadData.fileName,
            progress: 0,
            uploadedMB: 0,
            totalSizeMB: (uploadData.totalSize / (1024 * 1024)).toFixed(2),
            uploadedChunks: 0,
            totalChunks,
            status: 'processing',
            stage: 'file-processing',
            message: 'File processing started on server',
            userId
          });
        } catch (error) {
          console.warn('[Upload] Failed to emit initial progress via SSE:', error.message);
        }
      }

      // Process all chunks
      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, stats.size);
        const currentChunkSize = end - start;

        console.log(`[Upload] Queueing chunk ${i + 1}/${totalChunks} (${(currentChunkSize / (1024 * 1024)).toFixed(2)}MB)`);

        const chunkData = {
          fileId,
          fileName: file.originalname,
          filePath: file.path,
          fileSize: stats.size,
          chunkIndex: i,
          chunkSize: currentChunkSize,
          startByte: start,
          totalChunks,
          roomName,
          userId
        };

        // Process chunk asynchronously without waiting
        setImmediate(() => {
          sseSessionManager.processChunk(chunkData);
        });
      }

      console.log(`[Upload] File processing initiated successfully: ${fileId}`);
      res.json({
        fileId,
        totalChunks,
        fileName: file.originalname,
        fileSize: stats.size,
        fileSizeMB: (stats.size / (1024 * 1024)).toFixed(2),
        message: "File processing initiated successfully",
        tempDirectory: TEMP_UPLOAD_DIR
      });
    } catch (error) {
      console.error('[Upload] Error uploading file:', error.message);

      try {
        if (fs.existsSync(file.path)) {
          fs.unlinkSync(file.path);
          console.log(`[Cleanup] Deleted file on upload error: ${file.path}`);
        }
      } catch (cleanupError) {
        console.error('[Cleanup] Error deleting file on upload error:', cleanupError.message);
      }

      res.status(500).json({
        error: 'Failed to upload file',
        details: error.message,
        fileName: file.originalname
      });
    }
  });

  // =============================================================================
  // CLIENT-SIDE CHUNKED UPLOAD ENDPOINTS - PROFESSIONAL IMPLEMENTATION
  // =============================================================================

  // Store for tracking chunked uploads
  const chunkedUploads = new Map();

  // Client-side chunked upload class
  // Client-side chunked upload class
  class ChunkedUploadManager {
    constructor(fileId, fileName, fileSize, totalChunks, userId, roomName) {
      this.fileId = fileId;
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.totalChunks = totalChunks;
      this.userId = userId;
      this.roomName = roomName;
      this.chunks = new Map();
      this.uploadedChunks = 0;
      this.uploadedBytes = 0; // Actual bytes received (accurate tracking)
      this.startTime = Date.now();
      this.lastProgressUpdate = Date.now();
      this.tempFilePath = path.join(TEMP_UPLOAD_DIR, `${fileId}.tmp`);
      this.status = 'initialized';
      this.errors = [];
    }

    addChunk(chunkIndex, chunkBuffer) {
      if (!this.chunks.has(chunkIndex)) {
        this.chunks.set(chunkIndex, chunkBuffer);
        this.uploadedChunks++;
        this.uploadedBytes += chunkBuffer.length; // Accurate byte tracking

        console.log(`[ChunkedUpload] Added chunk ${chunkIndex}: ${chunkBuffer.length} bytes (Total: ${this.uploadedBytes}/${this.fileSize})`);

        // Broadcast progress update (throttled to avoid overwhelming SSE)
        const now = Date.now();
        if (now - this.lastProgressUpdate > 100) { // Update every 100ms
          this.broadcastProgress();
          this.lastProgressUpdate = now;
        }
      }
    }

    isComplete() {
      return this.uploadedChunks === this.totalChunks;
    }

    broadcastProgress() {
      const progress = (this.uploadedBytes / this.fileSize) * 100;
      const elapsed = (Date.now() - this.startTime) / 1000;
      const speed = elapsed > 0 ? this.uploadedBytes / elapsed : 0; // bytes per second

      try {
        realTimeBroadcaster.broadcast(this.roomName, "chunk-assembly-progress", {
          fileId: this.fileId,
          fileName: this.fileName,
          progress: Math.min(progress, 100),
          uploadedMB: (this.uploadedBytes / (1024 * 1024)).toFixed(2),
          totalSizeMB: (this.fileSize / (1024 * 1024)).toFixed(2),
          uploadedChunks: this.uploadedChunks,
          totalChunks: this.totalChunks,
          uploadedBytes: this.uploadedBytes,
          totalBytes: this.fileSize,
          status: this.status,
          stage: 'chunk-assembly',
          speed: speed,
          elapsed: elapsed,
          userId: this.userId,
          message: `Receiving chunks: ${this.uploadedChunks}/${this.totalChunks} (${progress.toFixed(1)}%)`
        });
      } catch (error) {
        console.warn('[ChunkedUpload] Failed to broadcast progress:', error.message);
      }
    }

    async assembleFile() {
      try {
        this.status = 'assembling';
        this.broadcastProgress();

        console.log(`[ChunkedUpload] Starting file assembly for ${this.fileName} (${this.totalChunks} chunks)`);

        const writeStream = fs.createWriteStream(this.tempFilePath);

        // Write chunks in order
        for (let i = 0; i < this.totalChunks; i++) {
          const chunk = this.chunks.get(i);
          if (!chunk) {
            throw new Error(`Missing chunk ${i}`);
          }
          writeStream.write(chunk);
        }

        writeStream.end();

        await new Promise((resolve, reject) => {
          writeStream.on('finish', resolve);
          writeStream.on('error', reject);
        });

        this.status = 'assembled';
        console.log(`[ChunkedUpload] File assembled successfully: ${this.fileName} (${this.fileSize} bytes)`);

        // Verify file size
        const stats = fs.statSync(this.tempFilePath);
        if (stats.size !== this.fileSize) {
          console.warn(`[ChunkedUpload] Size mismatch! Expected: ${this.fileSize}, Got: ${stats.size}`);
        }

        return this.tempFilePath;

      } catch (error) {
        this.status = 'error';
        this.errors.push(error.message);
        throw error;
      }
    }

    cleanup() {
      try {
        if (fs.existsSync(this.tempFilePath)) {
          fs.unlinkSync(this.tempFilePath);
          console.log(`[ChunkedUpload] Cleaned up temp file: ${this.tempFilePath}`);
        }
        this.chunks.clear();
      } catch (error) {
        console.error(`[ChunkedUpload] Cleanup error:`, error.message);
      }
    }

    // Safe cleanup that doesn't delete the file (for when file has been moved)
    cleanupWithoutFile() {
      try {
        this.chunks.clear();
        console.log(`[ChunkedUpload] Cleaned up chunks for: ${this.fileName}`);
      } catch (error) {
        console.error(`[ChunkedUpload] Cleanup error:`, error.message);
      }
    }
  }

  // Initialize chunked upload
  app.post('/init-chunked-upload', async (req, res) => {
    try {
      const { fileId, fileName, fileSize, totalChunks, userId, roomName } = req.body;

      if (!fileId || !fileName || !fileSize || !totalChunks || !userId || !roomName) {
        return res.status(400).json({ error: 'Missing required parameters' });
      }

      console.log(`[ChunkedUpload] Initializing: ${fileName} (${fileSize} bytes, ${totalChunks} chunks)`);

      const uploadManager = new ChunkedUploadManager(fileId, fileName, fileSize, totalChunks, userId, roomName);
      chunkedUploads.set(fileId, uploadManager);

      // Register with session manager
      if (userId) {
        sseSessionManager.addFileToUser(userId, fileId);
      }

      // Initialize server-side active upload entry so byte-progress can reflect total file progress immediately
      if (!sseSessionManager.activeUploads.has(fileId)) {
        sseSessionManager.activeUploads.set(fileId, {
          fileId,
          fileName,
          totalSize: fileSize,
          totalChunks,
          chunkSize: Math.ceil(fileSize / totalChunks),
          uploadedChunks: 0,
          uploadedBytes: 0,
          progress: 0,
          status: 'uploading',
          startTime: Date.now(),
          lastUpdate: Date.now(),
          roomName,
          userId,
          errors: []
        });
      }

      res.json({
        success: true,
        fileId,
        message: 'Chunked upload initialized'
      });

    } catch (error) {
      console.error('[ChunkedUpload] Init error:', error.message);
      res.status(500).json({ error: 'Failed to initialize chunked upload', details: error.message });
    }
  });

  // Upload individual chunk
  app.post('/upload-chunk', upload.single('chunk'), async (req, res) => {
    try {
      const { fileId, chunkIndex, totalChunks, fileName, fileSize, userId, roomName, chunkSize } = req.body;
      const chunkFile = req.file;

      if (!chunkFile || !fileId || chunkIndex == null) {
        return res.status(400).json({ error: 'Missing chunk data or fileId' });
      }

      const uploadManager = chunkedUploads.get(fileId);
      if (!uploadManager) {
        return res.status(404).json({ error: 'Upload session not found. Please reinitialize upload.' });
      }

      // Instead of buffering and assembling later, immediately process this chunk to Dropbox
      const actualChunkSize = fs.statSync(chunkFile.path).size;

      // Ensure server-side active upload entry exists (in case init step was missed)
      if (!sseSessionManager.activeUploads.has(fileId)) {
        const inferredTotalChunks = parseInt(totalChunks) || 1;
        const inferredFileSize = parseInt(fileSize) || actualChunkSize * inferredTotalChunks;
        sseSessionManager.activeUploads.set(fileId, {
          fileId,
          fileName: fileName || uploadManager.fileName,
          totalSize: inferredFileSize,
          totalChunks: inferredTotalChunks,
          chunkSize: Math.ceil(inferredFileSize / inferredTotalChunks),
          uploadedChunks: 0,
          uploadedBytes: 0,
          progress: 0,
          status: 'uploading',
          startTime: Date.now(),
          lastUpdate: Date.now(),
          roomName,
          userId,
          errors: []
        });
      }

      // Build chunkData for immediate server-side processing (client-origin)
      const immediateChunkData = {
        fileId,
        fileName: fileName || uploadManager.fileName,
        filePath: chunkFile.path,
        fileSize: parseInt(fileSize),
        chunkIndex: parseInt(chunkIndex),
        chunkSize: actualChunkSize,
        startByte: 0,
        totalChunks: parseInt(totalChunks),
        roomName,
        userId,
        fromClientChunk: true
      };

      // Kick off background upload to Dropbox and progress broadcasting
      setImmediate(() => {
        sseSessionManager.processChunk(immediateChunkData);
      });

      // Also update client-side receiver progress based on bytes received
      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData) {
        uploadData.uploadedChunks = (uploadData.uploadedChunks || 0) + 1;
        uploadData.uploadedBytes = (uploadData.uploadedBytes || 0) + actualChunkSize;
        uploadData.progress = Math.min(100, (uploadData.uploadedBytes / uploadData.totalSize) * 100);
        uploadData.lastUpdate = Date.now();

        const progressData = {
          fileId,
          fileName: uploadData.fileName,
          chunkIndex: parseInt(chunkIndex),
          totalChunks: uploadData.totalChunks,
          uploadedChunks: uploadData.uploadedChunks,
          uploadedBytes: uploadData.uploadedBytes,
          totalBytes: uploadData.totalSize,
          progress: uploadData.progress,
          speed: 0,
          userId,
          timestamp: Date.now(),
          stage: 'receiving'
        };

        try {
          realTimeBroadcaster.broadcast(roomName, "byte-progress", progressData);
        } catch (e) { }
      }

      res.json({
        success: true,
        chunkIndex: parseInt(chunkIndex),
        uploadedChunks: sseSessionManager.activeUploads.get(fileId)?.uploadedChunks || 0,
        totalChunks: sseSessionManager.activeUploads.get(fileId)?.totalChunks || parseInt(totalChunks),
        isComplete: sseSessionManager.activeUploads.get(fileId)?.uploadedChunks === parseInt(totalChunks),
        progress: sseSessionManager.activeUploads.get(fileId)?.progress || 0,
        uploadedBytes: sseSessionManager.activeUploads.get(fileId)?.uploadedBytes || actualChunkSize,
        totalBytes: sseSessionManager.activeUploads.get(fileId)?.totalSize || parseInt(fileSize)
      });

    } catch (error) {
      console.error('[ChunkedUpload] Chunk upload error:', error.message);
      res.status(500).json({ error: 'Failed to upload chunk', details: error.message });
    }
  });

  // Finalize chunked upload
  app.post('/finalize-chunked-upload', async (req, res) => {
    try {
      const { fileId, userId, roomName } = req.body;

      if (!fileId) {
        return res.status(400).json({ error: 'Missing fileId' });
      }

      const uploadManager = chunkedUploads.get(fileId);
      const activeUpload = sseSessionManager.activeUploads.get(fileId);

      // If we're in immediate-upload mode (no buffered chunks), acknowledge and return
      if (!uploadManager && activeUpload) {
        return res.json({
          success: true,
          fileId,
          message: 'Finalize acknowledged. Server is processing chunks directly.'
        });
      }

      if (!uploadManager) {
        return res.status(404).json({ error: 'Upload session not found' });
      }

      if (!uploadManager.isComplete()) {
        // For immediate-upload mode, allow finalize even if client-side manager not tracking chunks
        if (activeUpload) {
          return res.json({
            success: true,
            fileId,
            message: 'Finalize acknowledged. Server is processing chunks directly.'
          });
        }
        return res.status(400).json({
          error: 'Upload incomplete',
          uploadedChunks: uploadManager.uploadedChunks,
          totalChunks: uploadManager.totalChunks
        });
      }

      console.log(`[ChunkedUpload] Finalizing: ${uploadManager.fileName}`);

      // Assemble file
      const assembledFilePath = await uploadManager.assembleFile();

      // Create file object similar to multer
      const fileStats = fs.statSync(assembledFilePath);
      const mockFile = {
        originalname: uploadManager.fileName,
        filename: path.basename(assembledFilePath),
        path: assembledFilePath,
        size: fileStats.size,
        mimetype: 'application/octet-stream'
      };

      // Process with existing system
      const chunkSize = getOptimalChunkSize(fileStats.size);
      const totalChunks = Math.ceil(fileStats.size / chunkSize);

      const uploadData = {
        fileId,
        fileName: uploadManager.fileName,
        totalSize: fileStats.size,
        totalChunks,
        chunkSize,
        uploadedChunks: 0,
        uploadedBytes: 0,
        progress: 0,
        status: 'processing',
        filePath: assembledFilePath,
        startTime: uploadManager.startTime,
        errors: [],
        lastUpdate: Date.now(),
        roomName,
        userId
      };

      sseSessionManager.activeUploads.set(fileId, uploadData);

      // Start processing
      console.log(`[ChunkedUpload] Starting background processing for ${uploadManager.fileName}`);

      // Process asynchronously - FIXED: Don't cleanup until processing is complete
      setImmediate(async () => {
        try {
          await processUploadedFile(mockFile, uploadData, roomName, userId, uploadManager, fileId);

        } catch (error) {
          console.error('[ChunkedUpload] Processing error:', error.message);
          uploadManager.cleanup();
          chunkedUploads.delete(fileId);
        }
      });

      res.json({
        success: true,
        fileId,
        fileName: uploadManager.fileName,
        message: 'Upload finalized, processing started'
      });

    } catch (error) {
      console.error('[ChunkedUpload] Finalize error:', error.message);
      res.status(500).json({ error: 'Failed to finalize upload', details: error.message });
    }
  });

  // Helper function to process uploaded file using existing system - SIMPLIFIED
  async function processUploadedFile(file, uploadData, roomName, userId, uploadManager, fileId) {
    try {
      console.log(`[Processing] Starting direct processing for ${file.originalname}`);

      // Verify file exists before processing
      if (!fs.existsSync(file.path)) {
        throw new Error(`Assembled file not found: ${file.path}`);
      }

      const stats = fs.statSync(file.path);
      console.log(`[Processing] File verified: ${file.path} (${stats.size} bytes)`);

      // Create a permanent file path in temp directory
      const permanentPath = file.path.replace('.tmp', '');

      // Move the assembled file to permanent location
      fs.renameSync(file.path, permanentPath);
      console.log(`[Processing] File moved to permanent location: ${permanentPath}`);

      // Update file object with permanent path
      file.path = permanentPath;

      // Use the existing upload processing logic directly
      const chunkSize = getOptimalChunkSize(stats.size);
      const totalChunks = Math.ceil(stats.size / chunkSize);

      // Update upload data
      uploadData.chunkSize = chunkSize;
      uploadData.totalChunks = totalChunks;
      uploadData.filePath = permanentPath;

      // Process all chunks using existing system
      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, stats.size);
        const currentChunkSize = end - start;

        console.log(`[Processing] Queueing chunk ${i + 1}/${totalChunks} (${(currentChunkSize / (1024 * 1024)).toFixed(2)}MB)`);

        const chunkData = {
          fileId: uploadData.fileId,
          fileName: file.originalname,
          filePath: permanentPath,
          fileSize: stats.size,
          chunkIndex: i,
          chunkSize: currentChunkSize,
          startByte: start,
          totalChunks,
          roomName,
          userId
        };

        // Process chunk asynchronously using existing system
        setImmediate(() => {
          sseSessionManager.processChunk(chunkData);
        });
      }

      console.log(`[Processing] File processing initiated successfully: ${uploadData.fileId}`);

      // Cleanup chunked upload tracking (but keep the file for processing)
      if (uploadManager) {
        // Use safe cleanup that doesn't delete the file
        uploadManager.cleanupWithoutFile();
      }
      if (fileId) {
        chunkedUploads.delete(fileId);
      }

    } catch (error) {
      console.error('[Processing] Error:', error.message);

      // Cleanup on error
      if (uploadManager) {
        uploadManager.cleanup();
      }
      if (fileId) {
        chunkedUploads.delete(fileId);
      }

      throw error;
    }
  }

  app.get('/room/:roomName/get-file-chunk/:fileId', async (req, res) => {
    const { roomName, fileId } = req.params;
    const chunkIndex = req.query.index;

    if (chunkIndex == null) {
      return res.status(400).json({ error: 'Missing chunk index' });
    }

    await FileDownloader.streamFileChunk(roomName, fileId, chunkIndex, res);
  });

  app.get('/room/:roomName/get-file-metadata/:fileId', async (req, res) => {
    const { roomName, fileId } = req.params;

    try {
      const metadata = await FileDownloader.getFileMetadata(roomName, fileId);
      if (!metadata) {
        return res.status(404).json({ error: 'File not found' });
      }
      res.json(metadata);
    } catch (error) {
      console.error('[Metadata] Error retrieving file metadata:', error.message);
      res.status(500).json({ error: 'Failed to retrieve file metadata', details: error.message });
    }
  });

  app.get('/room/:roomName/get-content', async (req, res) => {
    const { roomName } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    try {
      const content = await ContentRetriever.getRoomContent(roomName, limit, offset);
      res.json(content);
    } catch (error) {
      console.error('[Content] Error retrieving room content:', error.message);
      res.status(500).json({ error: 'Failed to retrieve room content', details: error.message });
    }
  });

  app.get('/upload-progress/:fileId', async (req, res) => {
    const { fileId } = req.params;

    try {
      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData) {
        res.json(uploadData);
      } else {
        res.status(404).json({ error: 'Progress not found' });
      }
    } catch (error) {
      console.error('[Progress] Error retrieving upload progress:', error.message);
      res.status(500).json({ error: 'Failed to retrieve progress', details: error.message });
    }
  });

  // =============================================================================
  // SERVER-SENT EVENTS (SSE) & POLLING API ENDPOINTS
  // =============================================================================

  // CORS preflight handler for SSE endpoints
  app.options('/events/:roomName/:userId', (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Cache-Control, Last-Event-ID, Content-Type');
    res.setHeader('Access-Control-Max-Age', '86400'); // 24 hours
    res.status(200).end();
  });

  // SSE Connection endpoint - replaces Pusher channel subscription
  app.get('/events/:roomName/:userId', (req, res) => {
    const { roomName, userId } = req.params;

    // DETAILED LOGGING FOR SSE CONNECTION DEBUGGING
    console.log('='.repeat(80));
    console.log(`[SSE-DEBUG] NEW SSE CONNECTION ATTEMPT`);
    console.log(`[SSE-DEBUG] Timestamp: ${new Date().toISOString()}`);
    console.log(`[SSE-DEBUG] URL Path: ${req.path}`);
    console.log(`[SSE-DEBUG] Full URL: ${req.protocol}://${req.get('host')}${req.originalUrl}`);
    console.log(`[SSE-DEBUG] Method: ${req.method}`);
    console.log(`[SSE-DEBUG] Room Name: "${roomName}" (type: ${typeof roomName}, length: ${roomName ? roomName.length : 'N/A'})`);
    console.log(`[SSE-DEBUG] User ID: "${userId}" (type: ${typeof userId}, length: ${userId ? userId.length : 'N/A'})`);
    console.log(`[SSE-DEBUG] Client IP: ${req.ip || req.connection.remoteAddress || 'unknown'}`);
    console.log(`[SSE-DEBUG] User Agent: ${req.get('User-Agent') || 'unknown'}`);
    console.log(`[SSE-DEBUG] Origin: ${req.get('Origin') || 'none'}`);
    console.log(`[SSE-DEBUG] Referer: ${req.get('Referer') || 'none'}`);
    console.log(`[SSE-DEBUG] Accept: ${req.get('Accept') || 'none'}`);
    console.log(`[SSE-DEBUG] Cache-Control: ${req.get('Cache-Control') || 'none'}`);
    console.log(`[SSE-DEBUG] Connection: ${req.get('Connection') || 'none'}`);
    console.log(`[SSE-DEBUG] Headers:`, JSON.stringify(req.headers, null, 2));
    console.log(`[SSE-DEBUG] Query Params:`, JSON.stringify(req.query, null, 2));
    console.log('='.repeat(80));

    // Validate parameters with detailed logging
    if (!roomName) {
      console.error(`[SSE-ERROR] Missing or empty roomName parameter`);
      console.error(`[SSE-ERROR] roomName value: ${JSON.stringify(roomName)}`);
      console.error(`[SSE-ERROR] URL params:`, req.params);
      return res.status(400).json({
        error: 'Missing roomName parameter',
        received: { roomName, userId },
        params: req.params
      });
    }

    if (!userId) {
      console.error(`[SSE-ERROR] Missing or empty userId parameter`);
      console.error(`[SSE-ERROR] userId value: ${JSON.stringify(userId)}`);
      console.error(`[SSE-ERROR] URL params:`, req.params);
      return res.status(400).json({
        error: 'Missing userId parameter',
        received: { roomName, userId },
        params: req.params
      });
    }

    // Additional validation for empty strings or whitespace
    if (roomName.trim() === '') {
      console.error(`[SSE-ERROR] Room name is empty or whitespace only: "${roomName}"`);
      return res.status(400).json({
        error: 'Room name cannot be empty',
        received: { roomName, userId }
      });
    }

    if (userId.trim() === '') {
      console.error(`[SSE-ERROR] User ID is empty or whitespace only: "${userId}"`);
      return res.status(400).json({
        error: 'User ID cannot be empty',
        received: { roomName, userId }
      });
    }

    console.log(`[SSE-DEBUG] Parameter validation passed successfully`);
    console.log(`[SSE-DEBUG] Proceeding with SSE connection setup...`);

    try {
      console.log(`[SSE-DEBUG] Setting SSE headers...`);

      // Enhanced CORS Headers for SSE - CRITICAL for cross-origin SSE connections
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Cache-Control, Last-Event-ID, Content-Type, Accept');
      res.setHeader('Access-Control-Expose-Headers', 'Cache-Control, Content-Type');
      res.setHeader('Access-Control-Allow-Credentials', 'false');

      console.log(`[SSE-DEBUG] CORS headers set successfully`);

      // Enhanced SSE-specific headers for cloud deployment
      res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, no-transform');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('Keep-Alive', 'timeout=300'); // 5 minutes

      console.log(`[SSE-DEBUG] SSE-specific headers set successfully`);

      // Cloud platform optimizations
      res.setHeader('X-Accel-Buffering', 'no'); // Nginx
      res.setHeader('X-Proxy-Buffering', 'no'); // General proxy
      res.setHeader('Proxy-Buffering', 'no'); // Additional proxy
      res.setHeader('Transfer-Encoding', 'chunked'); // Force chunked encoding

      // Prevent all forms of buffering and caching
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      res.setHeader('Last-Modified', new Date().toUTCString());
      res.setHeader('ETag', `"sse-${Date.now()}"`);

      console.log(`[SSE-DEBUG] Cloud optimization headers set successfully`);
      console.log(`[SSE-DEBUG] All headers configured, setting status and flushing...`);

      // Force immediate header transmission
      res.status(200);

      console.log(`[SSE-DEBUG] Status 200 set, attempting to flush headers...`);

      try {
        res.flushHeaders();
        console.log(`[SSE-DEBUG] Headers flushed successfully`);
      } catch (flushError) {
        console.error(`[SSE-ERROR] Failed to flush headers:`, flushError.message);
        // Continue anyway, this might not be critical
      }

      console.log(`[SSE-DEBUG] Sending initial connection acknowledgment...`);

      // Send immediate connection acknowledgment to prevent timeouts
      try {
        res.write(': SSE connection established\n\n');
        console.log(`[SSE-DEBUG] Initial connection acknowledgment sent successfully`);
      } catch (writeError) {
        console.error(`[SSE-ERROR] Failed to write initial acknowledgment:`, writeError.message);
        throw writeError; // This is critical, throw to catch block
      }

      console.log(`[SSE-DEBUG] Adding user to room session...`);

      // Add user to room session
      try {
        console.log(`[SSE-DEBUG] Calling sseSessionManager.addUserToRoom("${userId}", "${roomName}")...`);
        sseSessionManager.addUserToRoom(userId, roomName);
        console.log(`[SSE-DEBUG] User successfully added to room session`);

        // Log current room stats
        const roomUsers = sseSessionManager.getRoomUsers(roomName);
        console.log(`[SSE-DEBUG] Room "${roomName}" now has ${roomUsers.size} users: [${Array.from(roomUsers).join(', ')}]`);

      } catch (sessionError) {
        console.error(`[SSE-ERROR] Failed to add user to room session:`, sessionError.message);
        console.error(`[SSE-ERROR] Session error stack:`, sessionError.stack);
        throw sessionError;
      }

      // Set connection type for health tracking
      try {
        console.log(`[SSE-DEBUG] Setting connection type for health tracking...`);
        const health = sseSessionManager.connectionHealth.get(userId);
        if (health) {
          health.connectionType = 'sse';
          console.log(`[SSE-DEBUG] Health tracking updated for user ${userId}`);
        } else {
          console.warn(`[SSE-WARNING] No health tracking found for user ${userId}`);
        }
      } catch (healthError) {
        console.error(`[SSE-ERROR] Failed to update health tracking:`, healthError.message);
        // Continue, this is not critical
      }

      // Create SSE connection
      try {
        console.log(`[SSE-DEBUG] Creating SSE connection via realTimeBroadcaster...`);
        console.log(`[SSE-DEBUG] realTimeBroadcaster status:`, {
          exists: !!realTimeBroadcaster,
          sseManager: !!realTimeBroadcaster?.sseManager,
          connectionCount: realTimeBroadcaster?.sseManager?.connections?.size || 0
        });

        const connection = realTimeBroadcaster.addSSEConnection(userId, roomName, res);

        if (connection) {
          console.log(`[SSE-DEBUG] SSE connection created successfully:`, {
            userId: connection.userId,
            roomName: connection.roomName,
            connected: connection.connected,
            lastHeartbeat: connection.lastHeartbeat
          });
        } else {
          console.error(`[SSE-ERROR] SSE connection creation returned null/undefined`);
        }

        // Log current connection stats
        const stats = realTimeBroadcaster.getStats();
        console.log(`[SSE-DEBUG] Current connection stats:`, JSON.stringify(stats, null, 2));

      } catch (connectionError) {
        console.error(`[SSE-ERROR] Failed to create SSE connection:`, connectionError.message);
        console.error(`[SSE-ERROR] Connection error stack:`, connectionError.stack);
        throw connectionError;
      }

      console.log('='.repeat(80));
      console.log(`[SSE-SUCCESS] User ${userId} connected to room "${roomName}" via SSE`);
      console.log(`[SSE-SUCCESS] Connection established at: ${new Date().toISOString()}`);
      console.log(`[SSE-SUCCESS] Connection should now be active and receiving events`);
      console.log('='.repeat(80));

    } catch (error) {
      console.log('='.repeat(80));
      console.error(`[SSE-FATAL] Error setting up SSE connection for user ${userId} in room "${roomName}"`);
      console.error(`[SSE-FATAL] Error message: ${error.message}`);
      console.error(`[SSE-FATAL] Error stack:`, error.stack);
      console.error(`[SSE-FATAL] Headers sent: ${res.headersSent}`);
      console.error(`[SSE-FATAL] Response finished: ${res.finished}`);
      console.error(`[SSE-FATAL] Response destroyed: ${res.destroyed}`);
      console.log('='.repeat(80));

      // Only send JSON response if headers haven't been sent yet
      if (!res.headersSent) {
        console.log(`[SSE-DEBUG] Headers not sent yet, sending error response...`);
        try {
          res.status(500).json({
            error: 'Failed to establish SSE connection',
            details: error.message,
            roomName,
            userId,
            timestamp: new Date().toISOString()
          });
          console.log(`[SSE-DEBUG] Error response sent successfully`);
        } catch (responseError) {
          console.error(`[SSE-ERROR] Failed to send error response:`, responseError.message);
        }
      } else {
        // Headers already sent, just close the connection
        console.log(`[SSE-DEBUG] Headers already sent, closing connection...`);
        try {
          res.end();
          console.log(`[SSE-DEBUG] Connection closed`);
        } catch (endError) {
          console.error(`[SSE-ERROR] Failed to close connection:`, endError.message);
        }
      }
    }
  });

  // Polling endpoint for fallback support
  app.get('/poll/:userId', (req, res) => {
    const { userId } = req.params;
    const since = parseInt(req.query.since) || 0;

    try {
      const events = realTimeBroadcaster.getPollingEvents(userId, since);

      // Update health tracking
      sseSessionManager.updateConnectionHealth(userId, 'poll');
      const health = sseSessionManager.connectionHealth.get(userId);
      if (health && health.connectionType === 'unknown') {
        health.connectionType = 'polling';
      }

      res.json({
        events,
        timestamp: Date.now(),
        userId
      });

    } catch (error) {
      console.error(`[Polling] Error getting events for ${userId}:`, error.message);
      res.status(500).json({ error: 'Failed to get events', details: error.message });
    }
  });

  // Verify SSE connection endpoint
  app.get('/verify-connection/:roomName/:userId', (req, res) => {
    const { roomName, userId } = req.params;

    try {
      const connection = realTimeBroadcaster.sseManager.connections.get(userId);
      const isConnected = connection && connection.connected && connection.roomName === roomName;

      res.json({
        connected: isConnected,
        roomName: roomName,
        userId: userId,
        connectionType: isConnected ? 'sse' : 'none',
        timestamp: Date.now()
      });
    } catch (error) {
      console.error(`[SSE] Error verifying connection for ${userId}:`, error.message);
      res.status(500).json({ error: 'Failed to verify connection', details: error.message });
    }
  });

  // Room joining via API endpoint - updated for SSE
  app.post('/join-room-sse', async (req, res) => {
    const { roomName, userId } = req.body;

    if (!roomName || !userId) {
      return res.status(400).json({ error: 'Missing roomName or userId' });
    }

    try {
      sseSessionManager.addUserToRoom(userId, roomName);

      console.log(`[SSE] User ${userId} joined room "${roomName}"`);

      res.json({
        message: 'Successfully joined room',
        roomName,
        userId,
        timestamp: Date.now(),
        sseEndpoint: `/events/${roomName}/${userId}`,
        pollEndpoint: `/poll/${userId}`
      });
    } catch (error) {
      console.error(`[SSE] Error joining room:`, error.message);
      res.status(500).json({ error: 'Failed to join room', details: error.message });
    }
  });

  // Verify connection endpoint - for SSE connection verification
  app.get('/verify-connection/:roomName/:userId', async (req, res) => {
    const { roomName, userId } = req.params;

    if (!roomName || !userId) {
      return res.status(400).json({ error: 'Missing roomName or userId' });
    }

    try {
      // Check if user is in the room
      const roomUsers = sseSessionManager.getRoomUsers(roomName);
      const userInRoom = roomUsers && roomUsers.has(userId);

      // Check if SSE connection exists
      const connection = realTimeBroadcaster.sseManager.connections.get(userId);
      const hasSSEConnection = connection && connection.connected;

      res.json({
        connected: hasSSEConnection,
        inRoom: userInRoom,
        roomName,
        userId,
        timestamp: Date.now(),
        connectionType: hasSSEConnection ? 'sse' : 'none'
      });
    } catch (error) {
      console.error(`[SSE] Error verifying connection:`, error.message);
      res.status(500).json({ error: 'Failed to verify connection', details: error.message });
    }
  });

  // Upload status request via API - updated for SSE
  app.get('/upload-status/:fileId', async (req, res) => {
    const { fileId } = req.params;
    const { userId } = req.query;

    try {
      console.log(`[SSE] Upload status requested for file ${fileId}`);
      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData) {
        res.json({
          ...uploadData,
          message: "Upload status retrieved"
        });
      } else {
        res.status(404).json({
          fileId,
          error: "Upload status not found",
          message: "File not currently being processed"
        });
      }
    } catch (error) {
      console.error(`[SSE] Error getting upload status:`, error.message);
      res.status(500).json({ fileId, error: error.message });
    }
  });

  // Real-time upload metrics via API - enhanced for client tracking
  app.get('/upload-metrics/:fileId', async (req, res) => {
    const { fileId } = req.params;
    const { userId } = req.query;

    try {
      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData) {
        const elapsed = Date.now() - uploadData.startTime;
        const speed = elapsed > 0 ? (uploadData.uploadedBytes / (elapsed / 1000)) : 0;
        const eta = uploadData.progress > 0 ? Math.round((elapsed / (uploadData.progress / 100)) - elapsed) / 1000 : null;

        res.json({
          fileId,
          fileName: uploadData.fileName,
          status: uploadData.status,
          progress: uploadData.progress || 0,
          uploadedChunks: uploadData.uploadedChunks || 0,
          totalChunks: uploadData.totalChunks,
          uploadedBytes: uploadData.uploadedBytes || 0,
          totalSize: uploadData.totalSize,
          uploadedMB: ((uploadData.uploadedBytes || 0) / (1024 * 1024)).toFixed(2),
          totalSizeMB: (uploadData.totalSize / (1024 * 1024)).toFixed(2),
          speed: Math.round(speed / 1024), // KB/s
          eta: eta ? Math.max(0, eta) : null,
          elapsed: Math.round(elapsed / 1000), // seconds
          errors: uploadData.errors || [],
          lastUpdate: uploadData.lastUpdate,
          isComplete: uploadData.status === 'completed',
          canHideIndicator: uploadData.status === 'completed' || uploadData.status === 'failed',
          userId,
          timestamp: Date.now()
        });
      } else {
        res.status(404).json({
          fileId,
          error: "Upload not found",
          canHideIndicator: true, // If upload not found, it's safe to hide indicator
          message: "File upload not currently active"
        });
      }
    } catch (error) {
      console.error(`[SSE] Error getting upload metrics:`, error.message);
      res.status(500).json({
        fileId,
        error: error.message,
        canHideIndicator: true // On error, allow hiding indicator
      });
    }
  });

  // Cancel upload via API - updated for SSE
  app.post('/cancel-upload/:fileId', async (req, res) => {
    const { fileId } = req.params;
    const { userId, roomName } = req.body;

    try {
      console.log(`[SSE] Upload cancellation requested for file ${fileId}`);

      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData && uploadData.filePath && fs.existsSync(uploadData.filePath)) {
        try {
          fs.unlinkSync(uploadData.filePath);
          console.log(`[Cleanup] Deleted temp file for cancelled upload: ${uploadData.filePath}`);
        } catch (cleanupError) {
          console.warn(`[Cleanup] Could not delete temp file:`, cleanupError.message);
        }
      }

      sseSessionManager.activeUploads.delete(fileId);
      sseSessionManager.waitingChunks.delete(fileId);

      // Send cancellation confirmation via SSE broadcast
      if (roomName) {
        realTimeBroadcaster.broadcast(roomName, "upload-cancelled", {
          fileId,
          userId,
          message: "Upload cancelled successfully",
          timestamp: Date.now()
        });
      }

      console.log(`[SSE] Upload cancelled for file ${fileId}`);
      res.json({
        fileId,
        message: "Upload cancelled successfully"
      });
    } catch (error) {
      console.error(`[SSE] Error cancelling upload:`, error.message);
      res.status(500).json({ fileId, error: error.message });
    }
  });

  // Byte progress request via API - updated for SSE
  app.get('/byte-progress/:fileId', async (req, res) => {
    const { fileId } = req.params;

    try {
      const uploadData = sseSessionManager.activeUploads.get(fileId);
      if (uploadData) {
        res.json({
          fileId,
          totalSize: uploadData.totalSize,
          chunkSize: uploadData.chunkSize,
          totalChunks: uploadData.totalChunks,
          uploadedBytes: uploadData.fileUploadedBytes || 0,
          progress: uploadData.progress || 0
        });
      } else {
        res.status(404).json({
          fileId,
          error: "Upload not found"
        });
      }
    } catch (error) {
      console.error(`[SSE] Error getting byte progress:`, error.message);
      res.status(500).json({ fileId, error: error.message });
    }
  });

  // Heartbeat/ping endpoint
  app.post('/ping', (req, res) => {
    res.json({
      pong: true,
      timestamp: Date.now(),
      message: "Server is alive"
    });
  });

  // Leave room endpoint
  app.post('/leave-room', async (req, res) => {
    const { roomName, userId } = req.body;

    if (!roomName || !userId) {
      return res.status(400).json({ error: 'Missing roomName or userId' });
    }

    try {
      sseSessionManager.removeUserFromRoom(userId);
      console.log(`[SSE] User ${userId} left room ${roomName}`);

      res.json({
        message: 'Successfully left room',
        roomName,
        userId
      });
    } catch (error) {
      console.error(`[SSE] Error leaving room:`, error.message);
      res.status(500).json({ error: 'Failed to leave room', details: error.message });
    }
  });

  // =============================================================================
  // ADMIN ENDPOINTS
  // =============================================================================
  app.get('/admin/dropbox-accounts', async (req, res) => {
    try {
      const accounts = await dropboxManager.fetchDropboxAccounts();
      const accountsWithStatus = await Promise.all(
        accounts.map(async (account) => {
          const isLocked = await dropboxManager.isAccountLocked(account.id);
          const used = parseFloat(account.fields.storage || "0");
          const free = DROPBOX_ACCOUNT_MAX_CAPACITY - used;
          return {
            id: account.id,
            name: account.fields.name || 'Unnamed',
            storageUsed: used,
            storageFree: free,
            storageUsedMB: (used / (1024 * 1024)).toFixed(2),
            storageFreeMB: (free / (1024 * 1024)).toFixed(2),
            storageUsedPercent: ((used / DROPBOX_ACCOUNT_MAX_CAPACITY) * 100).toFixed(1),
            isLocked,
            isEligible: free >= MIN_CHUNK_SIZE && !isLocked && used < DROPBOX_ROTATION_THRESHOLD
          };
        })
      );

      res.json({
        total: accountsWithStatus.length,
        eligible: accountsWithStatus.filter(acc => acc.isEligible).length,
        locked: accountsWithStatus.filter(acc => acc.isLocked).length,
        nearCapacity: accountsWithStatus.filter(acc => acc.storageUsed > DROPBOX_ROTATION_THRESHOLD).length,
        accounts: accountsWithStatus
      });
    } catch (error) {
      console.error('[Admin] Error retrieving Dropbox accounts status:', error.message);
      res.status(500).json({ error: 'Failed to retrieve Dropbox accounts status', details: error.message });
    }
  });

  app.get('/admin/sse-stats', async (req, res) => {
    try {
      console.log('[Admin] SSE connection stats requested');

      const connectionStats = realTimeBroadcaster.getStats();
      const sessionStats = sseSessionManager.getSessionStats();

      res.json({
        message: 'SSE statistics retrieved successfully',
        connections: connectionStats,
        sessions: sessionStats,
        serverInfo: {
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          platform: process.platform,
          nodeVersion: process.version
        },
        timestamp: Date.now()
      });
    } catch (error) {
      console.error('[Admin] Error retrieving SSE stats:', error.message);
      res.status(500).json({ error: 'Failed to retrieve SSE statistics', details: error.message });
    }
  });

  app.post('/admin/cleanup-connections', async (req, res) => {
    try {
      console.log('[Admin] Cleaning up stale SSE connections...');

      // Force cleanup of stale connections
      const connectionStats = realTimeBroadcaster.getStats();
      let cleanedCount = 0;

      // Manual cleanup trigger (connections are auto-cleaned, but we can force it)
      for (const [userId, connection] of realTimeBroadcaster.sseManager.connections) {
        const now = Date.now();
        if (now - connection.lastHeartbeat > 60000) { // 1 minute
          realTimeBroadcaster.sseManager.removeConnection(userId);
          cleanedCount++;
        }
      }

      res.json({
        message: 'Connection cleanup completed',
        cleanedConnections: cleanedCount,
        remainingConnections: realTimeBroadcaster.sseManager.connections.size,
        stats: realTimeBroadcaster.getStats()
      });
    } catch (error) {
      console.error('[Admin] Error cleaning up connections:', error.message);
      res.status(500).json({ error: 'Failed to cleanup connections', details: error.message });
    }
  });

  app.post('/admin/reset-sse-connections', async (req, res) => {
    try {
      console.log(`[Admin] Manually resetting SSE connections...`);

      // Get current stats before reset
      const beforeStats = realTimeBroadcaster.getStats();

      // Cleanup stale connections
      await realTimeBroadcaster.sseManager.startCleanup();

      // Get stats after reset
      const afterStats = realTimeBroadcaster.getStats();

      res.json({
        message: 'SSE connections reset successfully',
        before: beforeStats,
        after: afterStats,
        cleaned: beforeStats.totalConnections - afterStats.totalConnections
      });
    } catch (error) {
      console.error('[Admin] Error resetting SSE connections:', error.message);
      res.status(500).json({ error: 'Failed to reset SSE connections', details: error.message });
    }
  });

  app.post('/admin/sse-broadcast-test', async (req, res) => {
    try {
      const { roomName, eventType, testData, userId } = req.body;

      if (!roomName) {
        return res.status(400).json({ error: 'roomName is required' });
      }

      console.log('='.repeat(80));
      console.log(`[ADMIN-TEST] Testing SSE broadcast for room "${roomName}", event: "${eventType || 'test-event'}"`);
      console.log(`[ADMIN-TEST] Test initiated at: ${new Date().toISOString()}`);

      const beforeStats = realTimeBroadcaster.getStats();
      console.log(`[ADMIN-TEST] Before broadcast stats:`, JSON.stringify(beforeStats, null, 2));

      // Send test broadcast
      const broadcastData = testData || {
        message: 'Test broadcast from admin',
        timestamp: Date.now(),
        testId: `test-${Date.now()}`,
        adminGenerated: true
      };

      console.log(`[ADMIN-TEST] Broadcasting data:`, JSON.stringify(broadcastData, null, 2));

      realTimeBroadcaster.broadcast(roomName, eventType || 'test-event', broadcastData);

      const afterStats = realTimeBroadcaster.getStats();
      console.log(`[ADMIN-TEST] After broadcast stats:`, JSON.stringify(afterStats, null, 2));
      console.log('='.repeat(80));

      res.json({
        success: true,
        message: `Test broadcast sent to room "${roomName}"`,
        eventType: eventType || 'test-event',
        broadcastData,
        beforeStats,
        afterStats,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('[ADMIN-TEST] Error sending test broadcast:', error.message);
      res.status(500).json({ error: error.message });
    }
  });

  // Test endpoint to simulate a user joining a room (for testing)
  app.post('/admin/simulate-join', async (req, res) => {
    try {
      const { roomName, userId } = req.body;

      if (!roomName || !userId) {
        return res.status(400).json({ error: 'roomName and userId are required' });
      }

      console.log(`[ADMIN-SIMULATE] Simulating user "${userId}" joining room "${roomName}"`);

      // Add user to room via session manager
      sseSessionManager.addUserToRoom(userId, roomName);

      // Get updated stats
      const stats = realTimeBroadcaster.getStats();
      const sessionStats = sseSessionManager.getSessionStats();

      res.json({
        success: true,
        message: `User "${userId}" simulated join to room "${roomName}"`,
        roomName,
        userId,
        stats,
        sessionStats,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[ADMIN-SIMULATE] Error simulating join:', error.message);
      res.status(500).json({ error: error.message });
    }
  });

  // SSE Connection Diagnostics endpoint for debugging
  app.get('/admin/sse-diagnostics/:roomName?', async (req, res) => {
    try {
      const { roomName } = req.params;
      console.log(`[SSE-DIAGNOSTICS] Diagnostic request for room: ${roomName || 'ALL'}`);

      const sseStats = realTimeBroadcaster.sseManager.getStats();
      const sessionStats = sseSessionManager.getSessionStats();

      const diagnostics = {
        timestamp: new Date().toISOString(),
        serverInfo: {
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          pid: process.pid
        },
        sseManager: {
          totalConnections: realTimeBroadcaster.sseManager.connections.size,
          totalRooms: realTimeBroadcaster.sseManager.roomConnections.size,
          connectionDetails: {},
          roomDetails: {}
        },
        sessionManager: sessionStats,
        roomSpecific: null
      };

      // Detailed connection information
      for (const [userId, connection] of realTimeBroadcaster.sseManager.connections) {
        diagnostics.sseManager.connectionDetails[userId] = {
          roomName: connection.roomName,
          connected: connection.connected,
          lastHeartbeat: new Date(connection.lastHeartbeat).toISOString(),
          createdAt: connection.createdAt ? new Date(connection.createdAt).toISOString() : 'unknown',
          eventsSent: connection.eventsSent || 0,
          responseStatus: {
            destroyed: connection.res.destroyed,
            finished: connection.res.finished,
            headersSent: connection.res.headersSent,
            writable: connection.res.writable,
            writableEnded: connection.res.writableEnded
          }
        };
      }

      // Detailed room information
      for (const [room, users] of realTimeBroadcaster.sseManager.roomConnections) {
        diagnostics.sseManager.roomDetails[room] = {
          userCount: users.size,
          users: Array.from(users),
          activeConnections: Array.from(users).filter(userId =>
            realTimeBroadcaster.sseManager.connections.has(userId) &&
            realTimeBroadcaster.sseManager.connections.get(userId).connected
          )
        };
      }

      // Room-specific diagnostics
      if (roomName) {
        const roomUsers = realTimeBroadcaster.sseManager.roomConnections.get(roomName);
        diagnostics.roomSpecific = {
          roomName,
          exists: !!roomUsers,
          userCount: roomUsers ? roomUsers.size : 0,
          users: roomUsers ? Array.from(roomUsers) : [],
          sessionUsers: sseSessionManager.getRoomUsers(roomName),
          activeConnections: []
        };

        if (roomUsers) {
          for (const userId of roomUsers) {
            const connection = realTimeBroadcaster.sseManager.connections.get(userId);
            if (connection) {
              diagnostics.roomSpecific.activeConnections.push({
                userId,
                connected: connection.connected,
                lastHeartbeat: new Date(connection.lastHeartbeat).toISOString(),
                eventsSent: connection.eventsSent || 0,
                responseAlive: !connection.res.destroyed && !connection.res.finished
              });
            }
          }
        }
      }

      res.json(diagnostics);
    } catch (error) {
      console.error('[SSE-DIAGNOSTICS] Error generating diagnostics:', error.message);
      res.status(500).json({ error: 'Failed to generate diagnostics', details: error.message });
    }
  });

  app.get('/admin/system-info', async (req, res) => {
    try {
      const tempDirExists = fs.existsSync(TEMP_UPLOAD_DIR);
      let tempDirStats = null;
      let tempFiles = [];

      if (tempDirExists) {
        try {
          tempDirStats = fs.statSync(TEMP_UPLOAD_DIR);
          tempFiles = fs.readdirSync(TEMP_UPLOAD_DIR).map(file => {
            const filePath = path.join(TEMP_UPLOAD_DIR, file);
            const stats = fs.statSync(filePath);
            return {
              name: file,
              size: stats.size,
              sizeMB: (stats.size / (1024 * 1024)).toFixed(2),
              created: stats.birthtime,
              modified: stats.mtime,
              age: Date.now() - stats.mtime.getTime()
            };
          });
        } catch (error) {
          console.warn('[Admin] Error reading temp directory:', error.message);
        }
      }

      // Note: pusherManager was replaced with realTimeBroadcaster
      const sseStats = realTimeBroadcaster.getStats();

      res.json({
        system: {
          platform: process.platform,
          arch: process.arch,
          nodeVersion: process.version,
          pid: process.pid,
          uptime: process.uptime(),
          cpuCount: os.cpus().length,
          totalMemory: Math.round(os.totalmem() / 1024 / 1024) + 'MB',
          freeMemory: Math.round(os.freemem() / 1024 / 1024) + 'MB',
          loadAverage: os.loadavg(),
          homeDir: os.homedir(),
          tmpDir: os.tmpdir()
        },
        application: {
          version: '4.2.0-pusher-rotation',
          tempDirectory: TEMP_UPLOAD_DIR,
          tempDirExists,
          tempDirStats: tempDirStats ? {
            created: tempDirStats.birthtime,
            modified: tempDirStats.mtime,
            isDirectory: tempDirStats.isDirectory()
          } : null,
          tempFiles: tempFiles.length,
          tempFilesDetails: tempFiles.slice(0, 10), // Show first 10 files
          activeUploads: sseSessionManager.activeUploads.size,
          waitingChunks: Array.from(sessionManager.waitingChunks.values()).reduce((sum, chunks) => sum + chunks.length, 0),
          activeRooms: sessionManager.roomSessions.size
        },
        pusher: {
          totalAccounts: pusherStats.total,
          availableAccounts: pusherStats.available,
          limitedAccounts: pusherStats.limited,
          activeInstances: pusherTriggerSystem.activeInstances.size,
          failoverQueueLength: pusherTriggerSystem.failoverQueue.length,
          lastFetch: new Date(pusherManager.lastFetch).toISOString()
        },
        environment: {
          nodeEnv: process.env.NODE_ENV || 'development',
          port: process.env.PORT || process.env.X_ZOHO_CATALYST_LISTEN_PORT || 3000,
          hasAirtableKey: !!process.env.AIRTABLE_API_KEY,
          renderHosting: !!process.env.RENDER,
          herokuHosting: !!process.env.DYNO
        }
      });
    } catch (error) {
      console.error('[Admin] Error retrieving system info:', error.message);
      res.status(500).json({ error: 'Failed to retrieve system info', details: error.message });
    }
  });

  app.post('/admin/cleanup-temp-files', async (req, res) => {
    const { maxAgeHours = 24, confirm } = req.body;

    if (confirm !== 'yes-cleanup-temp-files') {
      return res.status(400).json({
        error: 'Confirmation required',
        message: 'Send { "confirm": "yes-cleanup-temp-files", "maxAgeHours": 24 } to confirm cleanup'
      });
    }

    try {
      console.log(`[Admin] Cleaning up temp files older than ${maxAgeHours} hours`);

      if (!fs.existsSync(TEMP_UPLOAD_DIR)) {
        return res.json({
          message: 'Temp directory does not exist',
          tempDirectory: TEMP_UPLOAD_DIR,
          cleaned: 0
        });
      }

      const files = fs.readdirSync(TEMP_UPLOAD_DIR);
      const now = Date.now();
      const maxAge = maxAgeHours * 60 * 60 * 1000;
      let cleaned = 0;
      const errors = [];

      for (const file of files) {
        const filePath = path.join(TEMP_UPLOAD_DIR, file);
        try {
          const stats = fs.statSync(filePath);
          if (now - stats.mtime.getTime() > maxAge) {
            fs.unlinkSync(filePath);
            cleaned++;
            console.log(`[Admin] Cleaned up temp file: ${file}`);
          }
        } catch (error) {
          errors.push({ file, error: error.message });
          console.error(`[Admin] Error cleaning up file ${file}:`, error.message);
        }
      }

      res.json({
        message: `Cleanup completed`,
        tempDirectory: TEMP_UPLOAD_DIR,
        totalFound: files.length,
        cleaned,
        errors: errors.length,
        errorDetails: errors,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[Admin] Error during temp files cleanup:', error.message);
      res.status(500).json({ error: 'Failed to cleanup temp files', details: error.message });
    }
  });

  // Simple test endpoint without Pusher
  app.post('/test-upload', upload.single('file'), async (req, res) => {
    try {
      const file = req.file;
      if (!file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      console.log(`[TestUpload] File received: ${file.originalname} (${file.size} bytes)`);

      // Clean up temp file
      if (fs.existsSync(file.path)) {
        fs.unlinkSync(file.path);
      }

      res.json({
        message: 'Test upload successful',
        fileName: file.originalname,
        fileSize: file.size
      });
    } catch (error) {
      console.error('[TestUpload] Error:', error.message);
      res.status(500).json({ error: 'Test upload failed', details: error.message });
    }
  });

  app.get('/', (req, res) => {
    res.json({
      message: 'Professional Infinity Share with Server-Sent Events (SSE)',
      version: '5.0.0-sse-optimized',
      worker: process.pid,
      status: 'running',
      tempDirectory: TEMP_UPLOAD_DIR,
      improvements: [
        'Server-Sent Events for real-time communication',
        'Polling fallback for maximum compatibility',
        'No external service dependencies',
        'Render-optimized architecture',
        'Professional connection management',
        'Enhanced error recovery',
        'Multi-protocol real-time support',
        'System-aware temp directory management'
      ],
      features: [
        'Server-Sent Events (SSE) Real-time Communication',
        'Polling Fallback System',
        'Direct Dropbox Account Management',
        'Dynamic Chunk Size Optimization',
        'Real-time Session Management',
        'Enhanced Error Recovery',
        'Neon DB Connection Rotation',
        'Dropbox Account Rotation',
        'Automatic Lock Management',
        'System-aware Temp Directory',
        'Professional Connection Health Monitoring'
      ],
      endpoints: {
        // Core endpoints
        health: 'GET /health',
        createRoom: 'POST /create-room',
        joinRoom: 'POST /join-room',
        postMessage: 'POST /room/:roomName/post-message',
        uploadFile: 'POST /room/:roomName/upload-file',
        getContent: 'GET /room/:roomName/get-content',
        downloadChunk: 'GET /room/:roomName/get-file-chunk/:fileId?index=N',
        fileMetadata: 'GET /room/:roomName/get-file-metadata/:fileId',
        uploadProgress: 'GET /upload-progress/:fileId',

        // SSE Real-time endpoints
        sseConnection: 'GET /events/:roomName/:userId',
        pollingFallback: 'GET /poll/:userId',
        joinRoomSSE: 'POST /join-room-sse',
        uploadStatus: 'GET /upload-status/:fileId',
        uploadMetrics: 'GET /upload-metrics/:fileId',
        cancelUpload: 'POST /cancel-upload/:fileId',
        byteProgress: 'GET /byte-progress/:fileId',
        leaveRoom: 'POST /leave-room',

        // Admin endpoints
        dropboxStatus: 'GET /admin/dropbox-accounts',
        sseStats: 'GET /admin/sse-stats',
        cleanupConnections: 'POST /admin/cleanup-connections',
        systemInfo: 'GET /admin/system-info',
        cleanupTempFiles: 'POST /admin/cleanup-temp-files'
      },
      realTimeEvents: {
        client: [
          'join-room-sse',
          'upload-status',
          'cancel-upload',
          'ping',
          'leave-room',
          'poll-events'
        ],
        server: [
          'connected',
          'heartbeat',
          'room-joined',
          'user-left',
          'upload-progress',
          'chunk-completed',
          'chunk-error',
          'byte-progress',
          'file-upload-complete',
          'upload-finished',
          'file-upload-failed',
          'upload-error',
          'upload-cancelled',
          'new-content'
        ]
      },
      sseSystem: {
        serverSentEvents: true,
        pollingFallback: true,
        connectionHealthMonitoring: true,
        automaticCleanup: true,
        multiProtocolSupport: true,
        renderOptimized: true
      }
    });
  });

  // Error handling middleware
  app.use((error, req, res, next) => {
    console.error('[Server] Unhandled error:', error.message);
    console.error('[Server] Error stack:', error.stack);

    // Don't try to send response if headers already sent (e.g., in SSE connections)
    if (res.headersSent) {
      console.error('[Server] Headers already sent, cannot send error response');
      return next(error);
    }

    if (error instanceof multer.MulterError) {
      if (error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          error: 'File too large',
          maxSize: '100GB',
          code: 'FILE_TOO_LARGE'
        });
      }
      if (error.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({
          error: 'Unexpected file field',
          code: 'UNEXPECTED_FILE'
        });
      }
      return res.status(400).json({
        error: error.message,
        code: error.code
      });
    }

    if (error.name === 'ValidationError') {
      return res.status(400).json({
        error: 'Validation failed',
        details: error.message,
        code: 'VALIDATION_ERROR'
      });
    }

    res.status(500).json({
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : 'An unexpected error occurred'
    });
  });

  // Handle 404s
  app.use((req, res) => {
    res.status(404).json({
      error: 'Endpoint not found',
      path: req.path,
      method: req.method,
      code: 'NOT_FOUND'
    });
  });

  // Graceful shutdown
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);
  process.on('uncaughtException', (error) => {
    console.error('[Process] Uncaught Exception:', error);
    gracefulShutdown('UNCAUGHT_EXCEPTION');
  });
  process.on('unhandledRejection', (reason, promise) => {
    console.error('[Process] Unhandled Rejection at:', promise, 'reason:', reason);
  });

  async function gracefulShutdown(signal) {
    console.log(`[Server] Received ${signal}, initiating graceful shutdown`);

    const shutdownTimeout = setTimeout(() => {
      console.log('[Server] Shutdown timeout exceeded, forcing exit');
      process.exit(1);
    }, 30000);

    try {
      server.close(async () => {
        console.log('[Server] HTTP server closed');

        // Close database connection pools
        console.log('[DB] Closing connection pools');
        for (const [key, pool] of connectionPools) {
          try {
            await pool.end();
            console.log(`[DB] Closed pool ${key}`);
          } catch (error) {
            console.error(`[DB] Error closing pool ${key}:`, error.message);
          }
        }

        // Clean up temp files (only very old ones during shutdown)
        console.log('[Cleanup] Cleaning temp directory on shutdown');
        try {
          if (fs.existsSync(TEMP_UPLOAD_DIR)) {
            const files = fs.readdirSync(TEMP_UPLOAD_DIR);
            for (const file of files) {
              const filePath = path.join(TEMP_UPLOAD_DIR, file);
              try {
                const stats = fs.statSync(filePath);
                // Only delete files older than 1 hour during shutdown
                if (Date.now() - stats.mtime.getTime() > 3600000) {
                  fs.unlinkSync(filePath);
                  console.log(`[Cleanup] Deleted old temp file: ${file}`);
                }
              } catch (err) {
                console.warn(`[Cleanup] Could not process ${filePath}:`, err.message);
              }
            }
          }
        } catch (cleanupError) {
          console.error(`[Cleanup] Error cleaning temp directory:`, cleanupError.message);
        }

        clearTimeout(shutdownTimeout);
        console.log('[Server] Graceful shutdown completed successfully');
        process.exit(0);
      });

    } catch (error) {
      console.error('[Server] Error initiating shutdown:', error.message);
      clearTimeout(shutdownTimeout);
      process.exit(1);
    }
  }

  // Start server
  const PORT = process.env.PORT || process.env.X_ZOHO_CATALYST_LISTEN_PORT || 3000;

  server.listen(PORT, async () => {
    console.log('='.repeat(80));
    console.log(`[Server] Professional Infinity Share with Server-Sent Events (SSE)`);
    console.log(`[Server] Server running on port ${PORT}`);
    console.log(`[Server] Worker PID: ${process.pid}`);
    console.log(`[Server] Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`[Server] Version: 5.0.0-sse-optimized`);
    console.log(`[Server] Platform: ${process.platform} ${process.arch}`);
    console.log('='.repeat(80));
    console.log(`[Config] Max chunk size: ${(MAX_CHUNK_SIZE / (1024 * 1024)).toFixed(2)}MB`);
    console.log(`[Config] Min chunk size: ${(MIN_CHUNK_SIZE / (1024 * 1024)).toFixed(2)}MB`);
    console.log(`[Config] Max Dropbox capacity: ${(DROPBOX_ACCOUNT_MAX_CAPACITY / (1024 * 1024 * 1024)).toFixed(2)}GB`);
    console.log(`[Config] DB rotation threshold: ${DB_ROTATION_THRESHOLD}MB`);
    console.log(`[Config] Dropbox rotation threshold: ${(DROPBOX_ROTATION_THRESHOLD / (1024 * 1024 * 1024)).toFixed(2)}GB`);
    console.log(`[Config] System temp base: ${SYSTEM_TEMP_BASE}`);
    console.log(`[Config] App temp directory: ${APP_TEMP_DIR}`);
    console.log(`[Config] User temp directory: ${USER_TEMP_DIR}`);
    console.log(`[Config] Active temp directory: ${TEMP_UPLOAD_DIR}`);
    console.log(`[Config] Concurrent uploads: ${MAX_CONCURRENT_UPLOADS}`);
    console.log(`[Config] Upload timeout: ${UPLOAD_TIMEOUT / 1000}s`);
    console.log('='.repeat(80));
    console.log('[Server] SERVER-SENT EVENTS (SSE) SYSTEM:');
    console.log('[Server] ✓ Real-time SSE communication');
    console.log('[Server] ✓ Polling fallback for maximum compatibility');
    console.log('[Server] ✓ Connection health monitoring');
    console.log('[Server] ✓ Automatic stale connection cleanup');
    console.log('[Server] ✓ Multi-protocol support (SSE + Polling)');
    console.log('[Server] ✓ Render-optimized architecture');
    console.log('[Server] ✓ No external service dependencies');
    console.log('='.repeat(80));
    console.log('[Server] ENHANCED FEATURES:');
    console.log('[Server] ✓ Unlimited file uploads with chunking');
    console.log('[Server] ✓ Real-time progress tracking via SSE');
    console.log('[Server] ✓ Dropbox account rotation');
    console.log('[Server] ✓ Neon database clustering');
    console.log('[Server] ✓ Professional error handling');
    console.log('[Server] ✓ Cloud deployment optimized');
    console.log('[Server] ✓ WebSocket-free real-time communication');
    console.log('='.repeat(80));
    console.log('[Server] Server startup completed successfully');
    console.log('='.repeat(80));
  });

  server.on('error', (error) => {
    console.error('[Server] Startup error:', error.message);
    process.exit(1);
  });
}

// Helper function to calculate optimal chunk size
function getOptimalChunkSize(fileSize) {
  if (fileSize > 10 * 1024 * 1024 * 1024) return 100 * 1024 * 1024; // 100MB for >10GB
  if (fileSize > 1 * 1024 * 1024 * 1024) return 50 * 1024 * 1024;   // 50MB for >1GB
  if (fileSize > 100 * 1024 * 1024) return 25 * 1024 * 1024;        // 25MB for >100MB
  if (fileSize > 10 * 1024 * 1024) return 10 * 1024 * 1024;         // 10MB for >10MB
  return Math.max(fileSize, 1024 * 1024); // Minimum 1MB or file size
}

// =============================================================================
// SSE ENHANCED SESSION MANAGER - REPLACING PUSHER ACCOUNT SYSTEM
// =============================================================================
class SSESessionManager {
  constructor(dropboxManager, storeChunkMetadata) {
    this.roomSessions = new Map();       // roomName -> Set of userIds
    this.userToRoom = new Map();         // userId -> roomName
    this.userFiles = new Map();          // userId -> Set of fileIds
    this.activeUploads = new Map();      // fileId -> upload data
    this.waitingChunks = new Map();      // fileId -> array of waiting chunks
    this.byteProgress = new Map();       // fileId -> chunkIndex -> progress
    this.connectionHealth = new Map();   // userId -> health metrics
    this.dropboxManager = dropboxManager; // Store reference to DropboxManager
    this.storeChunkMetadata = storeChunkMetadata; // Store reference to storeChunkMetadata function
    this.initialize();
  }

  initialize() {
    console.log('[SSESessionManager] Initializing SSE Session Management System...');
    this.startPeriodicCleanup();
    this.startHealthMonitoring();
  }

  addUserToRoom(userId, roomName) {
    if (!this.roomSessions.has(roomName)) {
      this.roomSessions.set(roomName, new Set());
    }
    this.roomSessions.get(roomName).add(userId);
    this.userToRoom.set(userId, roomName);

    // Initialize connection health tracking
    this.connectionHealth.set(userId, {
      joinedAt: Date.now(),
      lastActivity: Date.now(),
      messageCount: 0,
      uploadCount: 0,
      connectionType: 'unknown' // will be updated based on connection method
    });

    console.log(`[SSESession] User ${userId} joined room ${roomName}`);

    // Broadcast join event via SSE
    realTimeBroadcaster.broadcast(roomName, "room-joined", {
      userId,
      roomName,
      timestamp: Date.now(),
      message: "User joined room"
    }, userId);
  }

  removeUserFromRoom(userId) {
    const roomName = this.userToRoom.get(userId);
    if (roomName && this.roomSessions.has(roomName)) {
      this.roomSessions.get(roomName).delete(userId);
      if (this.roomSessions.get(roomName).size === 0) {
        this.roomSessions.delete(roomName);
      }
    }
    this.userToRoom.delete(userId);
    this.connectionHealth.delete(userId);

    console.log(`[SSESession] User ${userId} left room ${roomName}`);

    // Broadcast leave event via SSE
    if (roomName) {
      realTimeBroadcaster.broadcast(roomName, "user-left", {
        userId,
        roomName,
        timestamp: Date.now(),
        message: "User left room"
      }, userId);
    }
  }

  getRoomUsers(roomName) {
    return this.roomSessions.get(roomName) || new Set();
  }

  isRoomActive(roomName) {
    const users = this.getRoomUsers(roomName);
    return users.size > 0;
  }

  addFileToUser(userId, fileId) {
    if (!this.userFiles.has(userId)) {
      this.userFiles.set(userId, new Set());
    }
    this.userFiles.get(userId).add(fileId);
  }

  updateConnectionHealth(userId, activity) {
    const health = this.connectionHealth.get(userId);
    if (health) {
      health.lastActivity = Date.now();
      if (activity === 'message') health.messageCount++;
      if (activity === 'upload') health.uploadCount++;
    }
  }

  startPeriodicCleanup() {
    setInterval(() => {
      this.cleanupAbandonedUploads();
      this.cleanupOldTempFiles();
    }, 3600000); // Every hour
  }

  startHealthMonitoring() {
    setInterval(() => {
      const now = Date.now();
      const inactiveThreshold = 24 * 60 * 60 * 1000; // 24 hours

      for (const [userId, health] of this.connectionHealth) {
        if (now - health.lastActivity > inactiveThreshold) {
          console.log(`[SSESession] Cleaning up inactive user: ${userId}`);
          this.removeUserFromRoom(userId);
        }
      }
    }, 3600000); // Every hour
  }

  cleanupAbandonedUploads() {
    const now = Date.now();
    const threshold = 24 * 60 * 60 * 1000; // 24 hours

    for (const [fileId, uploadData] of this.activeUploads) {
      if (now - uploadData.startTime > threshold) {
        console.log(`[SSESession] Cleaning up abandoned upload: ${fileId}`);
        this.activeUploads.delete(fileId);
        this.waitingChunks.delete(fileId);
        this.byteProgress.delete(fileId);
      }
    }
  }

  cleanupOldTempFiles() {
    try {
      const files = fs.readdirSync(TEMP_UPLOAD_DIR);
      const now = Date.now();
      const threshold = 24 * 60 * 60 * 1000; // 24 hours

      for (const file of files) {
        const filePath = path.join(TEMP_UPLOAD_DIR, file);
        const stats = fs.statSync(filePath);
        if (now - stats.mtime.getTime() > threshold) {
          fs.unlinkSync(filePath);
          console.log(`[SSESession] Cleaned up old temp file: ${file}`);
        }
      }
    } catch (error) {
      console.error('[SSESession] Error cleaning temp files:', error.message);
    }
  }

  async processChunk(chunkData) {
    const { fileId, chunkIndex, userId } = chunkData;

    try {
      if (!fs.existsSync(chunkData.filePath)) {
        throw new Error(`Temp file missing: ${chunkData.filePath}`);
      }

      const stats = fs.statSync(chunkData.filePath);
      if (chunkData.startByte >= stats.size) {
        throw new Error(`Invalid chunk bounds`);
      }

      const eligibleAccounts = await this.dropboxManager.getEligibleAccounts(chunkData.chunkSize);

      if (eligibleAccounts.length === 0) {
        console.log(`[Upload] No eligible accounts, queuing chunk ${chunkIndex}`);
        this.addToWaitingChunks(fileId, chunkData);
        return;
      }

      let uploadResult = null;
      let lastError = null;

      for (const account of eligibleAccounts) {
        try {
          const onProgress = (progressInfo) => {
            this.updateByteProgress(fileId, chunkIndex, progressInfo, userId);
          };

          uploadResult = await this.dropboxManager.uploadChunkStream(account, chunkData, onProgress);
          break;
        } catch (error) {
          lastError = error;
          console.warn(`[Upload] Chunk ${chunkIndex} failed:`, error.message);
          continue;
        }
      }

      if (!uploadResult) {
        throw lastError || new Error(`All accounts failed for chunk ${chunkIndex}`);
      }

      await this.storeChunkMetadata({
        fileId,
        fileName: chunkData.fileName,
        chunkIndex,
        totalChunks: chunkData.totalChunks,
        roomName: chunkData.roomName,
        url: uploadResult.url,
        size: chunkData.chunkSize,
        checksum: uploadResult.checksum || "N/A"
      });

      await this.updateProgress(fileId, chunkIndex, chunkData.chunkSize, userId);

      const uploadData = this.activeUploads.get(fileId);
      if (uploadData && uploadData.uploadedChunks >= uploadData.totalChunks) {
        try {
          if (fs.existsSync(chunkData.filePath)) {
            fs.unlinkSync(chunkData.filePath);
            console.log(`[Cleanup] Deleted completed file: ${chunkData.filePath}`);
          }
        } catch (cleanupError) {
          console.error(`[Cleanup] Error deleting file:`, cleanupError.message);
        }
      }

    } catch (error) {
      console.error(`[Upload] Chunk error:`, error.message);
      await this.handleChunkError(fileId, chunkIndex, error, userId);
    }
  }

  updateByteProgress(fileId, chunkIndex, progressInfo, userId) {
    const session = this.activeUploads.get(fileId);
    if (!session) return;

    const uploadData = session;

    // Initialize file-wide byte counter
    uploadData.fileUploadedBytes = uploadData.fileUploadedBytes || 0;
    uploadData.totalSize = uploadData.totalSize || uploadData.fileSize;

    const delta = progressInfo.uploadedBytesSinceLast || 0;
    uploadData.fileUploadedBytes += delta;

    const totalProgress = (uploadData.fileUploadedBytes / uploadData.totalSize) * 100;
    const uploadedMB = (uploadData.fileUploadedBytes / (1024 * 1024)).toFixed(2);
    const speed = progressInfo.speed || 0;
    const eta = progressInfo.eta || null;

    // Send progress via SSE broadcast
    realTimeBroadcaster.broadcast(uploadData.roomName, "byte-progress", {
      fileId,
      uploadedBytes: uploadData.fileUploadedBytes,
      totalProgress,
      uploadedMB,
      speed,
      eta,
      userId
    });

    console.log(`[SSE] File-level byte-progress: fileId=${fileId}, uploaded=${uploadData.fileUploadedBytes}B, progress=${totalProgress.toFixed(1)}%`);
  }

  addToWaitingChunks(fileId, chunkData) {
    if (!this.waitingChunks.has(fileId)) {
      this.waitingChunks.set(fileId, []);
    }
    this.waitingChunks.get(fileId).push(chunkData);

    setTimeout(() => {
      this.processWaitingChunks(fileId);
    }, 30000); // Retry after 30 seconds
  }

  async processWaitingChunks(fileId) {
    const chunks = this.waitingChunks.get(fileId);
    if (!chunks || chunks.length === 0) return;

    const chunk = chunks[0];
    const eligibleAccounts = await this.dropboxManager.getEligibleAccounts(chunk.chunkSize);

    if (eligibleAccounts.length > 0) {
      this.waitingChunks.set(fileId, chunks.slice(1));
      await this.processChunk(chunk);
    } else {
      console.log(`[SSESession] No eligible accounts for waiting chunk, will retry later`);
    }
  }

  async updateProgress(fileId, chunkIndex, chunkSize, userId, progressData = null) {
    try {
      const uploadData = this.activeUploads.get(fileId);
      if (!uploadData) return;

      // Update chunk completion tracking
      uploadData.uploadedChunks = (uploadData.uploadedChunks || 0) + 1;
      uploadData.uploadedBytes = (uploadData.uploadedBytes || 0) + chunkSize;
      uploadData.progress = (uploadData.uploadedChunks / uploadData.totalChunks) * 100;
      uploadData.lastUpdate = Date.now();

      // Calculate timing and speed metrics
      const elapsed = Date.now() - uploadData.startTime;
      const uploadedMB = (uploadData.uploadedBytes / (1024 * 1024)).toFixed(2);
      const totalSizeMB = (uploadData.totalSize / (1024 * 1024)).toFixed(2);
      const speed = elapsed > 0 ? (uploadData.uploadedBytes / (elapsed / 1000)) : 0;
      const eta = uploadData.progress > 0 ? Math.round((elapsed / (uploadData.progress / 100)) - elapsed) / 1000 : null;

      // Broadcast chunk completion progress via SSE
      realTimeBroadcaster.broadcast(uploadData.roomName, "chunk-completed", {
        fileId,
        fileName: uploadData.fileName,
        chunkIndex,
        totalChunks: uploadData.totalChunks,
        uploadedChunks: uploadData.uploadedChunks,
        progress: uploadData.progress,
        uploadedMB,
        totalSizeMB,
        chunkSizeMB: (chunkSize / (1024 * 1024)).toFixed(2),
        speed: Math.round(speed / 1024), // KB/s
        eta: eta ? Math.max(0, eta) : null,
        status: 'uploading',
        userId,
        timestamp: Date.now()
      });

      // Broadcast server-side chunk processing progress (different from client upload progress)
      realTimeBroadcaster.broadcast(uploadData.roomName, "server-processing", {
        fileId,
        fileName: uploadData.fileName,
        chunkIndex,
        totalChunks: uploadData.totalChunks,
        uploadedChunks: uploadData.uploadedChunks,
        progress: uploadData.progress,
        uploadedMB,
        totalSizeMB,
        speed: Math.round(speed / 1024), // KB/s
        eta: eta ? Math.max(0, eta) : null,
        status: 'processing',
        stage: 'chunk-processing',
        userId,
        message: `Processing chunk ${chunkIndex + 1}/${uploadData.totalChunks}`
      });

      console.log(`[SSESession] Chunk ${chunkIndex + 1}/${uploadData.totalChunks} completed for ${fileId}: ${uploadData.progress.toFixed(1)}%`);

      // Check if all chunks are completed
      if (uploadData.uploadedChunks >= uploadData.totalChunks) {
        console.log(`[SSESession] All chunks completed for ${fileId}, marking file as complete`);
        await this.markFileComplete(fileId, userId);
      }

    } catch (error) {
      console.error(`[SSESession] Error updating progress:`, error.message);
    }
  }

  calculateETA(progressData) {
    const elapsed = Date.now() - progressData.startTime;
    const progress = progressData.progress / 100;

    if (progress > 0.01) {
      const estimatedTotal = elapsed / progress;
      const remaining = estimatedTotal - elapsed;
      return Math.max(0, Math.round(remaining / 1000));
    }

    return null;
  }

  async markFileComplete(fileId, userId) {
    try {
      const uploadData = this.activeUploads.get(fileId);
      if (!uploadData) return;

      // Update upload data to completed status
      uploadData.status = 'completed';
      uploadData.completedAt = Date.now();
      uploadData.progress = 100;

      const totalTime = Math.round((uploadData.completedAt - uploadData.startTime) / 1000); // seconds
      const averageSpeed = totalTime > 0 ? Math.round((uploadData.totalSize / totalTime) / 1024) : 0; // KB/s
      const totalSizeMB = (uploadData.totalSize / (1024 * 1024)).toFixed(2);

      // Broadcast comprehensive completion message via SSE
      realTimeBroadcaster.broadcast(uploadData.roomName, "file-upload-complete", {
        fileId,
        fileName: uploadData.fileName,
        fileSize: uploadData.totalSize,
        fileSizeMB: totalSizeMB,
        totalChunks: uploadData.totalChunks,
        uploadedChunks: uploadData.uploadedChunks,
        progress: 100,
        status: 'completed',
        totalTime,
        averageSpeed,
        userId,
        timestamp: uploadData.completedAt,
        message: `File "${uploadData.fileName}" uploaded successfully`,
        success: true
      });

      // Also broadcast a specific completion event for UI hiding
      realTimeBroadcaster.broadcast(uploadData.roomName, "upload-finished", {
        fileId,
        fileName: uploadData.fileName,
        status: 'completed',
        success: true,
        userId,
        hideIndicator: true, // Signal to hide upload indicator
        timestamp: uploadData.completedAt
      });

      console.log(`[SSESession] File upload completed: ${uploadData.fileName} (${totalSizeMB}MB in ${totalTime}s)`);

      // Broadcast new content notification
      realTimeBroadcaster.broadcast(uploadData.roomName, "new-content", {
        type: 'file',
        fileId,
        fileName: uploadData.fileName,
        fileSize: uploadData.totalSize,
        totalChunks: uploadData.totalChunks,
        userId,
        timestamp: uploadData.completedAt
      });

      // Clean up
      this.activeUploads.delete(fileId);
      this.waitingChunks.delete(fileId);
      this.byteProgress.delete(fileId);

      console.log(`[SSESession] File upload completed: ${fileId}`);

    } catch (error) {
      console.error(`[SSESession] Error marking file complete:`, error.message);
    }
  }

  async handleChunkError(fileId, chunkIndex, error, userId) {
    try {
      const uploadData = this.activeUploads.get(fileId);
      if (!uploadData) return;

      // Track error counts
      uploadData.errors = uploadData.errors || [];
      uploadData.errors.push({
        chunkIndex,
        error: error.message,
        timestamp: Date.now()
      });

      // Broadcast specific chunk error via SSE
      realTimeBroadcaster.broadcast(uploadData.roomName, "chunk-error", {
        fileId,
        fileName: uploadData.fileName,
        chunkIndex,
        totalChunks: uploadData.totalChunks,
        error: error.message,
        errorCount: uploadData.errors.length,
        userId,
        timestamp: Date.now()
      });

      // If too many errors, mark upload as failed
      if (uploadData.errors.length >= 5) {
        uploadData.status = 'failed';

        realTimeBroadcaster.broadcast(uploadData.roomName, "file-upload-failed", {
          fileId,
          fileName: uploadData.fileName,
          error: `Upload failed after ${uploadData.errors.length} errors`,
          lastError: error.message,
          userId,
          timestamp: Date.now()
        });

        // Also broadcast upload finished with failure status
        realTimeBroadcaster.broadcast(uploadData.roomName, "upload-finished", {
          fileId,
          fileName: uploadData.fileName,
          status: 'failed',
          success: false,
          error: error.message,
          userId,
          hideIndicator: true, // Signal to hide upload indicator
          timestamp: Date.now()
        });

        console.error(`[SSESession] File upload failed: ${uploadData.fileName} - ${error.message}`);

        // Clean up failed upload
        this.activeUploads.delete(fileId);
        this.waitingChunks.delete(fileId);
        this.byteProgress.delete(fileId);
      }

      console.log(`[SSESession] Chunk error for ${fileId}, chunk ${chunkIndex}: ${error.message}`);

    } catch (broadcastError) {
      console.error(`[SSESession] Error broadcasting chunk error:`, broadcastError.message);
    }
  }

  getSessionStats() {
    const roomStats = {};
    for (const [roomName, users] of this.roomSessions) {
      roomStats[roomName] = {
        userCount: users.size,
        users: Array.from(users)
      };
    }

    const connectionStats = {};
    for (const [userId, health] of this.connectionHealth) {
      connectionStats[userId] = {
        lastActivity: health.lastActivity,
        messageCount: health.messageCount,
        uploadCount: health.uploadCount,
        connectionType: health.connectionType
      };
    }

    return {
      totalRooms: this.roomSessions.size,
      totalUsers: this.userToRoom.size,
      activeUploads: this.activeUploads.size,
      roomStats,
      connectionStats
    };
  }
}

// Global SSE Session Manager Instance - declared above with realTimeBroadcaster

// NOTE: PusherTriggerSystem replaced by realTimeBroadcaster (SSE + Polling)
// All real-time functionality now handled by RealTimeEventBroadcaster class above

// =============================================================================
// START SERVER - Called after all classes are defined
// =============================================================================

// Start the server now that all classes are properly defined
if (!cluster.isMaster || process.env.NODE_ENV !== 'production') {
  startServer();
}