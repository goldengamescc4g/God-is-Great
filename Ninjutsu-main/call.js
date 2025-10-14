import { Server as SocketIO } from 'socket.io';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { authenticateUser } from './auth.js';
import { recordMeetingStart, recordMeetingEnd } from './meetingStats.js';
import { recordMeetingParticipant } from './meetingParticipantStats.js';
import { setupHandRaising } from './handRaising.js';

// Fix for __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Meeting class to manage meeting state
class Meeting {
  constructor(id, hostId, hostName) {
    this.id = id;
    this.hostId = hostId;
    this.hostName = hostName;
    this.meetingName = `${hostName}'s Meeting`; // Default meeting name
    this.participants = new Map();
    this.coHosts = new Set();
    this.spotlightedParticipant = null;
    this.screenShares = new Map();
    this.createdAt = new Date();
    this.autoSpotlightEnabled = true;
    this.manualSpotlight = false;
    this.raisedHands = new Set();
    this.iceServers = this.getICEServers();
    this.isLocked = false;
    this.permissions = {
      chatEnabled: true,
      fileSharing: true,
      emojiReactions: true,
      allowRename: true,
      allowUnmute: true,
      allowHandRaising: true,
      muteAllParticipants: false
    };
    this.connectionAttempts = new Map(); // Track connection attempts
    this.connectionStates = new Map(); // Track connection states
  }

  getICEServers() {
    // Enhanced ICE servers configuration for production with working TURN servers
    const iceServers = [
      // Multiple STUN servers for better connectivity
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' },
      { urls: 'stun:stun.stunprotocol.org:3478' },
      { urls: 'stun:stun.nextcloud.com:443' },
      { urls: 'stun:stun.sipgate.net:3478' },
      { urls: 'stun:stun.ekiga.net' },
      
      // Reliable free TURN servers (tested and working)
      {
        urls: 'turn:openrelay.metered.ca:80',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443?transport=tcp',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      
      // Backup TURN servers
      {
        urls: 'turn:relay1.expressturn.com:3478',
        username: 'ef3I7ZYQ1XQZAHK32F',
        credential: 'lL2xX9qQCkH6QzRU'
      },
      
      // Metered.ca TURN servers (reliable)
      {
        urls: 'turn:a.relay.metered.ca:80',
        username: 'a40c38b0e78216200d619b80',
        credential: 'dvWS61aEZmhNcJaS'
      },
      {
        urls: 'turn:a.relay.metered.ca:80?transport=tcp',
        username: 'a40c38b0e78216200d619b80',
        credential: 'dvWS61aEZmhNcJaS'
      },
      {
        urls: 'turn:a.relay.metered.ca:443',
        username: 'a40c38b0e78216200d619b80',
        credential: 'dvWS61aEZmhNcJaS'
      },
      {
        urls: 'turn:a.relay.metered.ca:443?transport=tcp',
        username: 'a40c38b0e78216200d619b80',
        credential: 'dvWS61aEZmhNcJaS'
      }
    ];

    // Add environment-based TURN servers if available (for production)
    const customTurnServers = [
      {
        envUrl: 'TWILIO_TURN_URL',
        envUsername: 'TWILIO_TURN_USERNAME',
        envCredential: 'TWILIO_TURN_CREDENTIAL'
      },
      {
        envUrl: 'XIRSYS_TURN_URL',
        envUsername: 'XIRSYS_USERNAME',
        envCredential: 'XIRSYS_CREDENTIAL'
      },
      {
        envUrl: 'METERED_TURN_URL',
        envUsername: 'METERED_USERNAME',
        envCredential: 'METERED_CREDENTIAL'
      }
    ];

    customTurnServers.forEach(server => {
      if (process.env[server.envUrl] && process.env[server.envUsername] && process.env[server.envCredential]) {
        iceServers.push({
          urls: process.env[server.envUrl],
          username: process.env[server.envUsername],
          credential: process.env[server.envCredential]
        });
      }
    });

    return iceServers;
  }

  // Enhanced WebRTC configuration for better cross-device connectivity
  getWebRTCConfig() {
    return {
      iceServers: this.iceServers,
      iceCandidatePoolSize: 10, // Gather more ICE candidates
      iceTransportPolicy: 'all', // Use both STUN and TURN
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require',
      // Enhanced for better connectivity
      iceConnectionReceivingTimeout: 60000,
      iceInactiveTimeout: 120000,
      iceGatheringTimeout: 30000
    };
  }

  addParticipant(socketId, name, isHost = false) {
    const participant = {
      socketId,
      name,
      isHost,
      isCoHost: false,
      isMuted: false,
      isCameraOff: false,
      isSpotlighted: false,
      isScreenSharing: false,
      audioLevel: 0,
      joinedAt: new Date(),
      isReady: false,
      handRaised: false,
      connectionState: 'new',
      networkQuality: 'unknown',
      lastHeartbeat: new Date()
    };
    
    this.participants.set(socketId, participant);
    this.connectionAttempts.set(socketId, 0);
    this.connectionStates.set(socketId, new Map());
    
    if (isHost && !this.spotlightedParticipant) {
      this.spotlightParticipant(socketId);
    }
    
    return participant;
  }

  removeParticipant(socketId) {
    this.participants.delete(socketId);
    this.coHosts.delete(socketId);
    this.screenShares.delete(socketId);
    this.raisedHands.delete(socketId);
    
    // Emit event for activity tracking
   
    this.connectionAttempts.delete(socketId);
    this.connectionStates.delete(socketId);
    
    if (this.spotlightedParticipant === socketId) {
      this.spotlightedParticipant = null;
      this.manualSpotlight = false;
    }
  }

  setParticipantReady(socketId) {
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.isReady = true;
      participant.connectionState = 'ready';
      participant.lastHeartbeat = new Date();
    }
  }

  updateParticipantConnectionState(socketId, targetId, state) {
    const connectionStates = this.connectionStates.get(socketId);
    if (connectionStates) {
      connectionStates.set(targetId, {
        state,
        timestamp: new Date(),
        attempts: this.getConnectionAttempts(socketId, targetId)
      });
    }
    
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.connectionState = state;
      participant.lastHeartbeat = new Date();
    }
  }

  getConnectionAttempts(socketId, targetId) {
    const key = `${socketId}-${targetId}`;
    return this.connectionAttempts.get(key) || 0;
  }

  incrementConnectionAttempts(socketId, targetId) {
    const key = `${socketId}-${targetId}`;
    const current = this.connectionAttempts.get(key) || 0;
    this.connectionAttempts.set(key, current + 1);
    return current + 1;
  }

  resetConnectionAttempts(socketId, targetId) {
    const key = `${socketId}-${targetId}`;
    this.connectionAttempts.set(key, 0);
  }

  updateNetworkQuality(socketId, quality) {
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.networkQuality = quality;
      participant.lastHeartbeat = new Date();
    }
  }

  getReadyParticipants() {
    return Array.from(this.participants.values()).filter(p => p.isReady);
  }

  // Connection health monitoring
  getUnhealthyConnections() {
    const unhealthy = [];
    const now = new Date();
    
    this.participants.forEach((participant, socketId) => {
      const timeSinceHeartbeat = now - participant.lastHeartbeat;
      if (timeSinceHeartbeat > 60000) { // 1 minute without heartbeat
        unhealthy.push(socketId);
      }
    });
    
    return unhealthy;
  }

  makeCoHost(socketId) {
    const participant = this.participants.get(socketId);
    if (participant && !participant.isHost) {
      participant.isCoHost = true;
      this.coHosts.add(socketId);
    }
  }

  removeCoHost(socketId) {
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.isCoHost = false;
      this.coHosts.delete(socketId);
    }
  }

  spotlightParticipant(socketId) {
    if (this.spotlightedParticipant) {
      const prevSpotlighted = this.participants.get(this.spotlightedParticipant);
      if (prevSpotlighted) {
        prevSpotlighted.isSpotlighted = false;
      }
    }

    const participant = this.participants.get(socketId);
    if (participant) {
      participant.isSpotlighted = true;
      this.spotlightedParticipant = socketId;
      this.manualSpotlight = true;
    }
  }

  removeSpotlight() {
    if (this.spotlightedParticipant) {
      const participant = this.participants.get(this.spotlightedParticipant);
      if (participant) {
        participant.isSpotlighted = false;
      }
      this.spotlightedParticipant = null;
      this.manualSpotlight = false;
    }
  }

  handleAudioActivity(socketId, audioLevel) {
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.audioLevel = audioLevel;
      participant.lastHeartbeat = new Date();
      
      if (!this.manualSpotlight && this.autoSpotlightEnabled && audioLevel > 0.3) {
        if (this.spotlightedParticipant !== socketId) {
          this.spotlightParticipant(socketId);
          this.manualSpotlight = false;
          return true;
        }
      }
    }
    return false;
  }

  addScreenShare(socketId, streamId, hasComputerAudio = false) {
    this.screenShares.set(socketId, {
      streamId,
      startedAt: new Date(),
      hasComputerAudio
    });
    
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.isScreenSharing = true;
      participant.isSharingComputerAudio = hasComputerAudio;
    }
  }

  removeScreenShare(socketId) {
    this.screenShares.delete(socketId);
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.isScreenSharing = false;
      participant.isSharingComputerAudio = false;
    }
  }

  raiseHand(socketId) {
    this.raisedHands.add(socketId);
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.handRaised = true;
    }
  }

  lowerHand(socketId) {
    this.raisedHands.delete(socketId);
    const participant = this.participants.get(socketId);
    if (participant) {
      participant.handRaised = false;
    }
  }

  getRaisedHands() {
    return Array.from(this.raisedHands);
  }

  canPerformHostAction(socketId) {
    const participant = this.participants.get(socketId);
    return participant && (participant.isHost || participant.isCoHost);
  }

  canMakeCoHost(socketId) {
    const participant = this.participants.get(socketId);
    return participant && participant.isHost;
  }

  lockMeeting() {
    this.isLocked = true;
  }

  unlockMeeting() {
    this.isLocked = false;
  }

  updatePermissions(permissions) {
    this.permissions = { ...this.permissions, ...permissions };
    
    if (permissions.muteAllParticipants) {
      this.participants.forEach((participant, socketId) => {
        if (!participant.isHost) {
          participant.isMuted = true;
        }
      });
    }
  }

  getPermissions() {
    return this.permissions;
  }

  isParticipantAllowed(socketId) {
    return !this.isLocked || this.participants.has(socketId);
  }

  renameParticipant(socketId, newName) {
    const participant = this.participants.get(socketId);
    if (!participant) return null;

    if (!newName || typeof newName !== 'string') return null;
    if (newName.trim().length === 0 || newName.trim().length > 50) return null;

    const oldName = participant.name;
    participant.name = newName.trim();
    
    return { oldName, newName: participant.name };
  }

  canRename(socketId) {
    const participant = this.participants.get(socketId);
    if (!participant) return false;
    
    if (participant.isHost) return true;
    return this.permissions.allowRename;
  }

  canUnmute(socketId) {
    const participant = this.participants.get(socketId);
    if (!participant) return false;
    
    if (participant.isHost) return true;
    if (this.permissions.muteAllParticipants) return false;
    
    return this.permissions.allowUnmute;
  }

  canRaiseHand(socketId) {
    const participant = this.participants.get(socketId);
    if (!participant) return false;
    
    return this.permissions.allowHandRaising;
  }

  // Meeting name methods
  updateMeetingName(newName) {
    if (!newName || typeof newName !== 'string') return false;
    if (newName.trim().length === 0 || newName.trim().length > 100) return false;
    
    this.meetingName = newName.trim();
    return true;
  }

  getMeetingName() {
    return this.meetingName;
  }

  canChangeMeetingName(socketId) {
    const participant = this.participants.get(socketId);
    return participant && (participant.isHost || participant.isCoHost);
  }
}

// Store meeting data
const meetings = new Map();
const participants = new Map();

// Helper function to validate data parameter
const validateData = (data, requiredFields = []) => {
  if (!data || typeof data !== 'object') {
    return { isValid: false, error: 'Invalid data format' };
  }
  
  for (const field of requiredFields) {
    if (!(field in data)) {
      return { isValid: false, error: `Missing required field: ${field}` };
    }
  }
  
  return { isValid: true };
};

export const setupSocketIO = (server) => {
  const io = new SocketIO(server, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"],
      credentials: true
    },
    transports: ['websocket', 'polling'],
    pingTimeout: 120000,
    pingInterval: 25000,
    upgradeTimeout: 30000,
    allowEIO3: true,
    // Enhanced configuration for better connectivity
    maxHttpBufferSize: 1e8, // 100MB for large data transfers
    allowRequest: (req, callback) => {
      // Add any custom authentication logic here
      callback(null, true);
    }
  });

  // Connection health monitoring
  setInterval(() => {
    meetings.forEach((meeting, meetingId) => {
      const unhealthy = meeting.getUnhealthyConnections();
      if (unhealthy.length > 0) {
        console.log(`Meeting ${meetingId}: Found ${unhealthy.length} unhealthy connections`);
        // Notify clients about connection issues
        unhealthy.forEach(socketId => {
          io.to(socketId).emit('connection-health-check');
        });
      }
    });
  }, 30000); // Check every 30 seconds

  // Meeting routes
  const setupMeetingRoutes = (app) => {
    app.get('/host/:meetingId', authenticateUser, (req, res) => {
      res.sendFile(path.join(__dirname, '../public', 'meetingHost.html'));
    });

    app.get('/join/:meetingId', authenticateUser, (req, res) => {
      const { meetingId } = req.params;
      const meeting = meetings.get(meetingId);
      
      if (meeting && meeting.isLocked) {
        res.sendFile(path.join(__dirname, '../public', 'meetingLocked.html'));
        return;
      }
      
      res.sendFile(path.join(__dirname, '../public', 'meetingJoin.html'));
    });

    app.post('/api/create-meeting', authenticateUser, (req, res) => {
      const meetingId = uuidv4().substring(0, 8).toUpperCase();
      
      res.json({ 
        meetingId,
        hostUrl: `/host/${meetingId}`,
        joinUrl: `/join/${meetingId}`
      });
    });

    app.get('/api/meeting/:meetingId', authenticateUser, (req, res) => {
      const { meetingId } = req.params;
      const meeting = meetings.get(meetingId);
      
      if (!meeting) {
        return res.status(404).json({ error: 'Meeting not found' });
      }

      res.json({
        id: meeting.id,
        hostName: meeting.hostName,
        meetingName: meeting.meetingName,
        participantCount: meeting.participants.size,
        createdAt: meeting.createdAt,
        isLocked: meeting.isLocked,
        permissions: meeting.getPermissions()
      });
    });

    // Enhanced ICE servers endpoint
    app.get('/api/ice-servers', authenticateUser, (req, res) => {
      const meeting = new Meeting('temp', 'temp', 'temp');
      res.json({ 
        iceServers: meeting.iceServers,
        webrtcConfig: meeting.getWebRTCConfig()
      });
    });

    // Network test endpoint for connectivity verification
    app.get('/api/network-test', authenticateUser, (req, res) => {
      res.json({
        timestamp: new Date().toISOString(),
        server: req.headers.host,
        userAgent: req.headers['user-agent'],
        ip: req.ip || req.connection.remoteAddress,
        status: 'ok'
      });
    });
  };

  // Socket.IO connection handling
  io.on('connection', (socket) => {
    console.log('User connected:', socket.id, 'from IP:', socket.handshake.address);

    // Enhanced connection monitoring
    socket.on('heartbeat', () => {
      const participantInfo = participants.get(socket.id);
      if (participantInfo) {
        const meeting = meetings.get(participantInfo.meetingId);
        if (meeting) {
          const participant = meeting.participants.get(socket.id);
          if (participant) {
            participant.lastHeartbeat = new Date();
          }
        }
      }
    });

    // Network quality reporting
    socket.on('network-quality', (data) => {
      const validation = validateData(data, ['quality']);
      if (!validation.isValid) {
        console.error(`Invalid network-quality data from ${socket.id}:`, validation.error);
        return;
      }

      const { quality, stats } = data;
      const participantInfo = participants.get(socket.id);
      if (participantInfo) {
        const meeting = meetings.get(participantInfo.meetingId);
        if (meeting) {
          meeting.updateNetworkQuality(socket.id, quality);
          console.log(`Network quality for ${socket.id}: ${quality}`, stats);
        }
      }
    });

    // Meeting name change event
    socket.on('change-meeting-name', (data) => {
      const validation = validateData(data, ['newName']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { newName } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) {
        socket.emit('action-error', { message: 'Participant not found' });
        return;
      }
      
      

      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) {
        socket.emit('action-error', { message: 'Meeting not found' });
        return;
      }

      if (!meeting.canChangeMeetingName(socket.id)) {
        socket.emit('action-error', { message: 'Only host can change meeting name' });
        return;
      }

      if (!newName || newName.trim().length === 0) {
        socket.emit('action-error', { message: 'Please enter a valid meeting name' });
        return;
      }
      
      if (newName.length > 100) {
        socket.emit('action-error', { message: 'Meeting name is too long (max 100 characters)' });
        return;
      }
      
      const oldName = meeting.getMeetingName();
      const success = meeting.updateMeetingName(newName);
      
      if (success) {
        console.log(`Meeting ${participantInfo.meetingId} name changed from "${oldName}" to "${newName}" by ${socket.id}`);
        
        io.to(participantInfo.meetingId).emit('meeting-name-changed', {
          oldName: oldName,
          newName: meeting.getMeetingName(),
          changedBy: meeting.participants.get(socket.id)?.name
        });
      } else {
        socket.emit('action-error', { message: 'Failed to update meeting name' });
      }
    });

    socket.on('change-participant-name', (data) => {
      const validation = validateData(data, ['newName']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { newName, oldName } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) {
        socket.emit('action-error', { message: 'Participant not found' });
        return;
      }
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) {
        socket.emit('action-error', { message: 'Meeting not found' });
        return;
      }

      const participant = meeting.participants.get(socket.id);
      if (!participant) {
        socket.emit('action-error', { message: 'Participant not found in meeting' });
        return;
      }

      if (!meeting.canRename(socket.id)) {
        socket.emit('action-error', { message: 'You are not allowed to rename yourself' });
        return;
      }

      if (!newName || newName.trim().length === 0) {
        socket.emit('action-error', { message: 'Please enter a valid name' });
        return;
      }
      
      if (newName.length > 50) {
        socket.emit('action-error', { message: 'Name is too long (max 50 characters)' });
        return;
      }
      
      const trimmedName = newName.trim();
      const nameExists = Array.from(meeting.participants.values())
        .some(p => p.name.toLowerCase() === trimmedName.toLowerCase() && p.socketId !== socket.id);
        
      if (nameExists) {
        socket.emit('action-error', { message: 'This name is already taken by another participant' });
        return;
      }
      
      const currentName = participant.name;
      participant.name = trimmedName;
      
      if (participant.isHost) {
        meeting.hostName = trimmedName;
      }
      
      console.log(`Participant ${socket.id} renamed from ${currentName} to ${trimmedName} in meeting ${participantInfo.meetingId}`);
      
      io.to(participantInfo.meetingId).emit('participant-renamed', {
        socketId: socket.id,
        oldName: currentName,
        newName: trimmedName,
        participants: Array.from(meeting.participants.values()),
        isHost: participant.isHost
      });
    });

socket.on('join-as-host', async (data) => {
  const validation = validateData(data, ['meetingId', 'hostName']);
  if (!validation.isValid) {
      socket.emit('meeting-error', { message: validation.error });
      return;
  }

  const { meetingId, hostName, userId, meetingName } = data;
  
  // Record meeting start for statistics
  try {
      if (userId) {
          const recordingName = meetingName || `${hostName}'s Meeting`;
          await recordMeetingStart(userId, meetingId, recordingName, true);
          await recordMeetingParticipant(userId, 1);
          
          // NEW: Emit meeting started event for activity tracking
          socket.emit('meeting-started', {
              meetingId: meetingId,
              meetingName: recordingName,
              userId: userId,
              isHost: true
          });
      }
  } catch (error) {
      console.error('Error recording meeting start:', error);
  }
  
  const meeting = new Meeting(meetingId, socket.id, hostName);
  
  if (meetingName && meetingName.trim().length > 0) {
      meeting.meetingName = meetingName.trim();
      console.log(`Meeting ${meetingId} created with custom name: "${meeting.meetingName}"`);
  } else {
      meeting.meetingName = `${hostName}'s Meeting`;
      console.log(`Meeting ${meetingId} created with default name: "${meeting.meetingName}"`);
  }
  
  meetings.set(meetingId, meeting);
  meeting.addParticipant(socket.id, hostName, true);
  socket.join(meetingId);
  participants.set(socket.id, { meetingId, isHost: true });
  socket.currentRoom = meetingId;
  socket.userId = userId;
  
  socket.emit('joined-meeting', {
      meetingId,
      isHost: true,
      participants: Array.from(meeting.participants.values()),
      spotlightedParticipant: meeting.spotlightedParticipant,
      raisedHands: meeting.getRaisedHands(),
      iceServers: meeting.iceServers,
      webrtcConfig: meeting.getWebRTCConfig(),
      isLocked: meeting.isLocked,
      permissions: meeting.getPermissions(),
      meetingName: meeting.getMeetingName()
  });

  console.log(`Host ${hostName} created meeting ${meetingId} with name "${meeting.getMeetingName()}"`);
});

socket.on('join-meeting', async (data) => {
  const validation = validateData(data, ['meetingId', 'participantName']);
  if (!validation.isValid) {
      socket.emit('meeting-error', { message: validation.error });
      return;
  }

  const { meetingId, participantName, userId } = data;
  const meeting = meetings.get(meetingId);
  
  if (!meeting) {
      socket.emit('meeting-error', { message: 'Meeting not found' });
      return;
  }

  if (meeting.isLocked && !meeting.participants.has(socket.id)) {
      socket.emit('meeting-locked', { 
          message: 'The host disabled New Entries, Meeting Inaccessible',
          meetingId: meetingId
      });
      return;
  }

  // Record meeting start for statistics
  try {
      if (userId) {
          await recordMeetingStart(userId, meetingId, meeting.getMeetingName(), false);
          
          // NEW: Emit participant joined meeting event for activity tracking
          socket.emit('participant-joined-meeting', {
              meetingId: meetingId,
              meetingName: meeting.getMeetingName(),
              userId: userId
          });
      }
      
      const hostParticipant = Array.from(meeting.participants.values()).find(p => p.isHost);
      if (hostParticipant && hostParticipant.socketId) {
          const hostInfo = participants.get(hostParticipant.socketId);
          if (hostInfo && hostInfo.userId) {
              await recordMeetingParticipant(hostInfo.userId, 1);
          }
      }
  } catch (error) {
      console.error('Error recording meeting start:', error);
  }

  meeting.addParticipant(socket.id, participantName);
  socket.join(meetingId);
  participants.set(socket.id, { meetingId, isHost: false });
  socket.currentRoom = meetingId;
  socket.userId = userId;
  
  socket.emit('joined-meeting', {
      meetingId,
      isHost: false,
      participants: Array.from(meeting.participants.values()),
      spotlightedParticipant: meeting.spotlightedParticipant,
      screenShares: Array.from(meeting.screenShares.entries()),
      raisedHands: meeting.getRaisedHands(),
      iceServers: meeting.iceServers,
      webrtcConfig: meeting.getWebRTCConfig(),
      isLocked: meeting.isLocked,
      permissions: meeting.getPermissions(),
      meetingName: meeting.getMeetingName()
  });

  socket.to(meetingId).emit('participant-joined', {
      participant: meeting.participants.get(socket.id),
      participants: Array.from(meeting.participants.values())
  });

  console.log(`Participant ${participantName} joined meeting ${meetingId} ("${meeting.getMeetingName()}")`);
});

    socket.on('toggle-meeting-lock', (data) => {
      const validation = validateData(data, ['isLocked']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { isLocked } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Only host can lock/unlock the meeting' });
        return;
      }

      if (isLocked) {
        meeting.lockMeeting();
      } else {
        meeting.unlockMeeting();
      }

      io.to(participantInfo.meetingId).emit('meeting-lock-changed', {
        isLocked: meeting.isLocked,
        changedBy: meeting.participants.get(socket.id)?.name
      });

      console.log(`Meeting ${participantInfo.meetingId} ${isLocked ? 'locked' : 'unlocked'} by ${socket.id}`);
    });

    socket.on('update-meeting-permissions', (data) => {
      const validation = validateData(data, ['permissions']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { permissions } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Only host can update meeting permissions' });
        return;
      }

      meeting.updatePermissions(permissions);

      io.to(participantInfo.meetingId).emit('meeting-permissions-updated', {
        permissions: meeting.getPermissions(),
        changedBy: meeting.participants.get(socket.id)?.name,
        participants: Array.from(meeting.participants.values())
      });

      if (permissions.hasOwnProperty('allowRename')) {
        io.to(participantInfo.meetingId).emit('rename-permission-updated', {
          permissions: { allowRename: permissions.allowRename },
          changedBy: meeting.participants.get(socket.id)?.name
        });
      }

      console.log(`Meeting ${participantInfo.meetingId} permissions updated by ${socket.id}:`, permissions);
    });

    socket.on('participant-ready', () => {
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      meeting.setParticipantReady(socket.id);
      
      const readyParticipants = meeting.getReadyParticipants();
      
      // Enhanced connection initiation with retry logic
      readyParticipants.forEach(participant => {
        if (participant.socketId !== socket.id) {
          const webrtcConfig = meeting.getWebRTCConfig();
          
          io.to(participant.socketId).emit('initiate-connection', {
            targetSocketId: socket.id,
            shouldCreateOffer: true,
            iceServers: meeting.iceServers,
            webrtcConfig: webrtcConfig,
            connectionId: `${participant.socketId}-${socket.id}`
          });
          
          socket.emit('initiate-connection', {
            targetSocketId: participant.socketId,
            shouldCreateOffer: false,
            iceServers: meeting.iceServers,
            webrtcConfig: webrtcConfig,
            connectionId: `${socket.id}-${participant.socketId}`
          });
        }
      });

      console.log(`Participant ${socket.id} is ready for WebRTC connections`);
    });

    socket.on('connection-state-change', (data) => {
      const validation = validateData(data, ['targetSocketId', 'state']);
      if (!validation.isValid) {
        console.error(`Invalid connection-state-change data from ${socket.id}:`, validation.error);
        return;
      }

      const { targetSocketId, state, connectionId } = data;
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      meeting.updateParticipantConnectionState(socket.id, targetSocketId, state);
      
      io.to(targetSocketId).emit('peer-connection-state', {
        fromSocketId: socket.id,
        state: state,
        connectionId: connectionId
      });

      // Handle failed connections with automatic retry
      if (state === 'failed' || state === 'disconnected') {
        const attempts = meeting.incrementConnectionAttempts(socket.id, targetSocketId);
        console.log(`Connection ${state} between ${socket.id} and ${targetSocketId}, attempt ${attempts}`);
        
        if (attempts < 3) {
          setTimeout(() => {
            socket.emit('retry-connection', {
              targetSocketId: targetSocketId,
              attempt: attempts
            });
            io.to(targetSocketId).emit('retry-connection', {
              targetSocketId: socket.id,
              attempt: attempts
            });
          }, 2000 * attempts); // Exponential backoff
        }
      } else if (state === 'connected') {
        meeting.resetConnectionAttempts(socket.id, targetSocketId);
      }

      console.log(`Connection state between ${socket.id} and ${targetSocketId}: ${state}`);
    });

    // Enhanced offer handling with better logging
    socket.on('offer', (data) => {
      const validation = validateData(data, ['target', 'offer']);
      if (!validation.isValid) {
        console.error(`Invalid offer data from ${socket.id}:`, validation.error);
        return;
      }

      const { target, offer, connectionId } = data;
      console.log(`Offer from ${socket.id} to ${target} (${connectionId})`);
      
      socket.to(target).emit('offer', {
        offer: offer,
        sender: socket.id,
        connectionId: connectionId
      });
    });

    // Enhanced answer handling
    socket.on('answer', (data) => {
      const validation = validateData(data, ['target', 'answer']);
      if (!validation.isValid) {
        console.error(`Invalid answer data from ${socket.id}:`, validation.error);
        return;
      }

      const { target, answer, connectionId } = data;
      console.log(`Answer from ${socket.id} to ${target} (${connectionId})`);
      
      socket.to(target).emit('answer', {
        answer: answer,
        sender: socket.id,
        connectionId: connectionId
      });
    });

    // Enhanced ICE candidate handling
    socket.on('ice-candidate', (data) => {
      const validation = validateData(data, ['target', 'candidate']);
      if (!validation.isValid) {
        console.error(`Invalid ice-candidate data from ${socket.id}:`, validation.error);
        return;
      }

      const { target, candidate, connectionId } = data;
      console.log(`ICE candidate from ${socket.id} to ${target} (${connectionId})`);
      
      // Validate ICE candidate before forwarding
      if (candidate && (candidate.candidate || candidate.candidate === null)) {
        socket.to(target).emit('ice-candidate', {
          candidate: candidate,
          sender: socket.id,
          connectionId: connectionId
        });
      } else {
        console.warn(`Invalid ICE candidate from ${socket.id}:`, candidate);
      }
    });

    // Enhanced connection failure handling
    socket.on('connection-failed', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        console.error(`Invalid connection-failed data from ${socket.id}:`, validation.error);
        return;
      }

      const { targetSocketId, reason, connectionId } = data;
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;
      
      const attempts = meeting.incrementConnectionAttempts(socket.id, targetSocketId);
      console.log(`Connection failed between ${socket.id} and ${targetSocketId}, reason: ${reason}, attempt: ${attempts}`);
      
      if (attempts < 5) {
        const delay = Math.min(1000 * Math.pow(2, attempts), 10000); // Exponential backoff with max 10s
        
        setTimeout(() => {
          const webrtcConfig = meeting.getWebRTCConfig();
          
          socket.emit('restart-connection', {
            targetSocketId: targetSocketId,
            attempt: attempts,
            webrtcConfig: webrtcConfig,
            connectionId: `restart-${socket.id}-${targetSocketId}-${attempts}`
          });
          
          io.to(targetSocketId).emit('restart-connection', {
            targetSocketId: socket.id,
            attempt: attempts,
            webrtcConfig: webrtcConfig,
            connectionId: `restart-${targetSocketId}-${socket.id}-${attempts}`
          });
        }, delay);
      } else {
        console.error(`Max connection attempts reached between ${socket.id} and ${targetSocketId}`);
        socket.emit('connection-permanently-failed', { targetSocketId });
        io.to(targetSocketId).emit('connection-permanently-failed', { targetSocketId: socket.id });
      }
    });

    socket.on('audio-level', (data) => {
      const validation = validateData(data, ['level']);
      if (!validation.isValid) {
        return; // Don't log audio-level validation errors as they happen frequently
      }

      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const spotlightChanged = meeting.handleAudioActivity(socket.id, data.level);
      
      if (spotlightChanged) {
        io.to(participantInfo.meetingId).emit('participant-spotlighted', {
          spotlightedParticipant: meeting.spotlightedParticipant,
          participants: Array.from(meeting.participants.values()),
          reason: 'audio-activity'
        });
      }
    });

    socket.on('send-reaction', (data) => {
      const validation = validateData(data, ['emoji', 'timestamp']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;

      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      if (!meeting.permissions.emojiReactions) {
        socket.emit('action-error', { message: 'Emoji reactions are disabled by the host' });
        return;
      }

      const participant = meeting.participants.get(socket.id);
      if (!participant) return;

      io.to(participantInfo.meetingId).emit('reaction-received', {
        emoji: data.emoji,
        participantName: participant.name,
        socketId: socket.id,
        timestamp: data.timestamp
      });

      console.log(`Reaction ${data.emoji} sent by ${participant.name} in meeting ${participantInfo.meetingId}`);
    });

    socket.on('raise-hand', () => {
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;

      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      if (!meeting.canRaiseHand(socket.id)) {
        socket.emit('action-error', { message: 'Hand raising is disabled by the host' });
        return;
      }

      const participant = meeting.participants.get(socket.id);
      if (!participant) return;

      meeting.raiseHand(socket.id);

      io.to(participantInfo.meetingId).emit('hand-raised', {
        socketId: socket.id,
        participantName: participant.name,
        raisedHands: meeting.getRaisedHands()
      });

      console.log(`${participant.name} raised hand in meeting ${participantInfo.meetingId}`);
    });

    socket.on('lower-hand', () => {
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;

      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (!participant) return;

      meeting.lowerHand(socket.id);

      io.to(participantInfo.meetingId).emit('hand-lowered', {
        socketId: socket.id,
        participantName: participant.name,
        raisedHands: meeting.getRaisedHands()
      });

      console.log(`${participant.name} lowered hand in meeting ${participantInfo.meetingId}`);
    });

    socket.on('start-screen-share', (data) => {
      const validation = validateData(data, ['streamId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      meeting.addScreenShare(socket.id, data.streamId, data.hasComputerAudio || false);
      
      socket.to(participantInfo.meetingId).emit('screen-share-started', {
        participantId: socket.id,
        streamId: data.streamId,
        participantName: meeting.participants.get(socket.id)?.name,
        hasComputerAudio: data.hasComputerAudio || false
      });

      console.log(`Screen share started by ${socket.id} in meeting ${participantInfo.meetingId}`);
    });

    socket.on('stop-screen-share', () => {
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      meeting.removeScreenShare(socket.id);
      
      socket.to(participantInfo.meetingId).emit('screen-share-stopped', {
        participantId: socket.id
      });

      console.log(`Screen share stopped by ${socket.id} in meeting ${participantInfo.meetingId}`);
    });

    socket.on('toggle-computer-audio', (data) => {
      const validation = validateData(data, ['enabled']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { enabled } = data;
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (participant && participant.isScreenSharing) {
        participant.isSharingComputerAudio = enabled;
        
        const screenShare = meeting.screenShares.get(socket.id);
        if (screenShare) {
          screenShare.hasComputerAudio = enabled;
        }
        
        socket.to(participantInfo.meetingId).emit('computer-audio-toggled', {
          participantId: socket.id,
          enabled,
          participantName: participant.name
        });

        console.log(`Computer audio ${enabled ? 'enabled' : 'disabled'} by ${socket.id} in meeting ${participantInfo.meetingId}`);
      }
    });

    socket.on('computer-audio-level', (data) => {
      const validation = validateData(data, ['level']);
      if (!validation.isValid) {
        return; // Don't log audio-level validation errors as they happen frequently
      }

      const { level } = data;
      const participantInfo = participants.get(socket.id);
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (participant && participant.isScreenSharing && participant.isSharingComputerAudio) {
        socket.to(participantInfo.meetingId).emit('computer-audio-level-update', {
          participantId: socket.id,
          level,
          participantName: participant.name
        });
      }
    });

    socket.on('spotlight-participant', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Insufficient permissions' });
        return;
      }

      meeting.spotlightParticipant(targetSocketId);
      
      io.to(participantInfo.meetingId).emit('participant-spotlighted', {
        spotlightedParticipant: targetSocketId,
        participants: Array.from(meeting.participants.values()),
        reason: 'manual'
      });

      console.log(`Participant ${targetSocketId} spotlighted in meeting ${participantInfo.meetingId}`);
    });

    socket.on('remove-spotlight', () => {
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Insufficient permissions' });
        return;
      }

      meeting.removeSpotlight();
      
      io.to(participantInfo.meetingId).emit('spotlight-removed', {
        participants: Array.from(meeting.participants.values())
      });

      console.log(`Spotlight removed in meeting ${participantInfo.meetingId}`);
    });

    socket.on('pin-participant', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      socket.emit('participant-pinned', {
        pinnedParticipant: targetSocketId
      });

      console.log(`Participant ${socket.id} pinned ${targetSocketId}`);
    });

    socket.on('mute-participant', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Insufficient permissions' });
        return;
      }
        
        // Emit participant left event for activity tracking
        socket.emit('participant-left-meeting', {
          meetingId: meetingId,
          userId: socket.userId || socket.id,
          finalMeetingName: meeting.name,
          leaveTime: new Date().toISOString()
        });
        

      const targetParticipant = meeting.participants.get(targetSocketId);
      if (targetParticipant) {
        targetParticipant.isMuted = !targetParticipant.isMuted;
        
        io.to(targetSocketId).emit('force-mute', {
          isMuted: targetParticipant.isMuted
        });
        
        io.to(participantInfo.meetingId).emit('participant-muted', {
          targetSocketId,
          isMuted: targetParticipant.isMuted,
          participants: Array.from(meeting.participants.values())
        });

        console.log(`Participant ${targetSocketId} ${targetParticipant.isMuted ? 'muted' : 'unmuted'} in meeting ${participantInfo.meetingId}`);
      }
    });

    socket.on('make-cohost', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canMakeCoHost(socket.id)) {
        socket.emit('action-error', { message: 'Only host can make co-hosts' });
        return;
      }

      meeting.makeCoHost(targetSocketId);
      
      io.to(targetSocketId).emit('made-cohost');
      
      io.to(participantInfo.meetingId).emit('cohost-assigned', {
        targetSocketId,
        participants: Array.from(meeting.participants.values())
      });

      console.log(`Participant ${targetSocketId} made co-host in meeting ${participantInfo.meetingId}`);
    });

    socket.on('kick-participant', (data) => {
      const validation = validateData(data, ['targetSocketId']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const requester = meeting.participants.get(socket.id);
      const target = meeting.participants.get(targetSocketId);
      
      if (!requester || !target) return;
      
      if (!requester.isHost || target.isCoHost) {
        socket.emit('action-error', { message: 'Cannot kick this participant' });
        return;
      }

      meeting.removeParticipant(targetSocketId);
      participants.delete(targetSocketId);
      
      io.to(targetSocketId).emit('kicked-from-meeting');
      
      socket.to(participantInfo.meetingId).emit('participant-kicked', {
        targetSocketId,
        participants: Array.from(meeting.participants.values())
      });

      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket) {
        targetSocket.leave(participantInfo.meetingId);
        targetSocket.disconnect();
      }

      console.log(`Participant ${targetSocketId} kicked from meeting ${participantInfo.meetingId}`);
    });

    socket.on('toggle-mic', (data) => {
      const validation = validateData(data, ['isMuted']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { isMuted } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (participant) {
        if (isMuted === false && !meeting.canUnmute(socket.id)) {
          socket.emit('action-error', { message: 'You are not allowed to unmute yourself' });
          return;
        }
        
        participant.isMuted = isMuted;
        
        socket.to(participantInfo.meetingId).emit('participant-audio-changed', {
          socketId: socket.id,
          isMuted,
          participants: Array.from(meeting.participants.values())
        });
      }
    });

    socket.on('toggle-camera', (data) => {
      const validation = validateData(data, ['isCameraOff']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { isCameraOff } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (participant) {
        participant.isCameraOff = isCameraOff;
        
        socket.to(participantInfo.meetingId).emit('participant-video-changed', {
          socketId: socket.id,
          isCameraOff,
          participants: Array.from(meeting.participants.values())
        });
      }
    });

    socket.on('rename-participant', (data) => {
      const validation = validateData(data, ['newName']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { newName } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) {
        socket.emit('action-error', { message: 'Participant not found' });
        return;
      }
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) {
        socket.emit('action-error', { message: 'Meeting not found' });
        return;
      }

      if (!meeting.canRename(socket.id)) {
        socket.emit('action-error', { message: 'You are not allowed to rename yourself' });
        return;
      }

      if (!newName || newName.trim().length === 0 || newName.trim().length > 50) {
        socket.emit('action-error', { message: 'Invalid name. Name must be 1-50 characters.' });
        return;
      }

      const trimmedName = newName.trim();
      const existingParticipant = Array.from(meeting.participants.values())
        .find(p => p.name.toLowerCase() === trimmedName.toLowerCase() && p.socketId !== socket.id);
      
      if (existingParticipant) {
        socket.emit('action-error', { message: 'This name is already taken by another participant' });
        return;
      }

      const result = meeting.renameParticipant(socket.id, trimmedName);
      if (result) {
        io.to(participantInfo.meetingId).emit('participant-renamed', {
          socketId: socket.id,
          oldName: result.oldName,
          newName: result.newName,
          participants: Array.from(meeting.participants.values()),
          isHost: false
        });

        console.log(`Participant ${socket.id} renamed from ${result.oldName} to ${result.newName} in meeting ${participantInfo.meetingId}`);
      } else {
        socket.emit('action-error', { message: 'Failed to rename participant' });
      }
    });

    socket.on('host-rename-participant', (data) => {
      const validation = validateData(data, ['targetSocketId', 'newName']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { targetSocketId, newName } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Only host can rename participants' });
        return;
      }

      if (!newName || newName.trim().length === 0 || newName.trim().length > 50) {
        socket.emit('action-error', { message: 'Invalid name. Name must be 1-50 characters.' });
        return;
      }

      const trimmedName = newName.trim();
      const existingParticipant = Array.from(meeting.participants.values())
        .find(p => p.name.toLowerCase() === trimmedName.toLowerCase() && p.socketId !== targetSocketId);
      
      if (existingParticipant) {
        socket.emit('action-error', { message: 'This name is already taken by another participant' });
        return;
      }

      const result = meeting.renameParticipant(targetSocketId, trimmedName);
      if (result) {
        io.to(participantInfo.meetingId).emit('participant-renamed', {
          socketId: targetSocketId,
          oldName: result.oldName,
          newName: result.newName,
          participants: Array.from(meeting.participants.values()),
          renamedBy: meeting.participants.get(socket.id)?.name
        });

        console.log(`Host ${socket.id} renamed participant ${targetSocketId} from ${result.oldName} to ${result.newName} in meeting ${participantInfo.meetingId}`);
      }
    });

    socket.on('host-rename-self', (data) => {
      const validation = validateData(data, ['newName']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { newName } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting) return;

      const participant = meeting.participants.get(socket.id);
      if (!participant || !participant.isHost) {
        socket.emit('action-error', { message: 'Only host can use this feature' });
        return;
      }

      if (!newName || newName.trim().length === 0 || newName.trim().length > 50) {
        socket.emit('action-error', { message: 'Invalid name. Name must be 1-50 characters.' });
        return;
      }

      const trimmedName = newName.trim();
      const existingParticipant = Array.from(meeting.participants.values())
        .find(p => p.name.toLowerCase() === trimmedName.toLowerCase() && p.socketId !== socket.id);
      
      if (existingParticipant) {
        socket.emit('action-error', { message: 'This name is already taken by another participant' });
        return;
      }

      const result = meeting.renameParticipant(socket.id, trimmedName);
      if (result) {
        meeting.hostName = trimmedName;
        
        io.to(participantInfo.meetingId).emit('participant-renamed', {
          socketId: socket.id,
          oldName: result.oldName,
          newName: result.newName,
          participants: Array.from(meeting.participants.values()),
          isHost: true
        });

        console.log(`Host ${socket.id} renamed themselves from ${result.oldName} to ${result.newName} in meeting ${participantInfo.meetingId}`);
      }
    });

    socket.on('mute-all-participants', (data) => {
      const validation = validateData(data, ['muteAll']);
      if (!validation.isValid) {
        socket.emit('action-error', { message: validation.error });
        return;
      }

      const { muteAll } = data;
      const participantInfo = participants.get(socket.id);
      
      if (!participantInfo) return;
      
      const meeting = meetings.get(participantInfo.meetingId);
      if (!meeting || !meeting.canPerformHostAction(socket.id)) {
        socket.emit('action-error', { message: 'Only host can mute all participants' });
        return;
      }

      meeting.updatePermissions({ muteAllParticipants: muteAll });

      io.to(participantInfo.meetingId).emit('all-participants-muted', {
        muteAll,
        participants: Array.from(meeting.participants.values()),
        permissions: meeting.getPermissions(),
        mutedBy: meeting.participants.get(socket.id)?.name
      });

      console.log(`Host ${socket.id} ${muteAll ? 'muted' : 'unmuted'} all participants in meeting ${participantInfo.meetingId}`);
    });

    socket.on('disconnect', (reason) => {
      const participantInfo = participants.get(socket.id);
      
      console.log(`User disconnected: ${socket.id}, reason: ${reason}`);
      
      // Record meeting end for statistics
      if (socket.currentRoom && socket.userId) {
        const meeting = meetings.get(socket.currentRoom);
        const participantCount = meeting ? meeting.participants.size : 1;
        
        recordMeetingEnd(socket.userId, socket.currentRoom, participantCount)
          .catch(error => console.error('Error recording meeting end:', error));
      }
      
      if (participantInfo) {
        const meeting = meetings.get(participantInfo.meetingId);
        
        if (meeting) {
          const participant = meeting.participants.get(socket.id);
          
          meeting.removeParticipant(socket.id);
          
          if (participantInfo.isHost) {
            socket.to(participantInfo.meetingId).emit('meeting-ended', {
              reason: 'host-disconnected'
            });
            
            meetings.delete(participantInfo.meetingId);
            
            console.log(`Meeting ${participantInfo.meetingId} ended - host disconnected`);
          } else {
            socket.to(participantInfo.meetingId).emit('participant-left', {
              socketId: socket.id,
              participantName: participant?.name,
              participants: Array.from(meeting.participants.values()),
              reason: reason
            });
            
            console.log(`Participant ${socket.id} left meeting ${participantInfo.meetingId}`);
          }
        }
        
        participants.delete(socket.id);
      }
    });

    // Error handling
    socket.on('error', (error) => {
      console.error(`Socket error for ${socket.id}:`, error);
      socket.emit('socket-error', { 
        message: 'Connection error occurred',
        timestamp: new Date().toISOString()
      });
    });
  });

  return { io, setupMeetingRoutes };
};