class WebRTCManager {
        constructor(socket) {
          this.socket = socket;
          this.localStream = null;
          this.screenStream = null;
          this.peerConnections = new Map();
          this.remoteStreams = new Map();
          this.isScreenSharing = false;
          this.audioContext = null;
          this.isReady = false;
          this.originalMicrophoneTrack = null;
          this.streamAttachRetries = new Map(); // Track retry attempts
          this.meetingStartTime = null;
          this.meetingName = '';
          
          this.configuration = {
            iceServers: [
              { urls: 'stun:stun.l.google.com:19302' },
              { urls: 'stun:stun1.l.google.com:19302' }
            ]
          };

          this.setupSocketListeners();
        }

        setupSocketListeners() {
          this.socket.on('initiate-connection', async (data) => {
            const { targetSocketId, shouldCreateOffer } = data;
            console.log(`Initiating connection with ${targetSocketId}, shouldCreateOffer: ${shouldCreateOffer}`);
            
            if (shouldCreateOffer) {
              await this.createPeerConnection(targetSocketId, true);
            } else {
              await this.createPeerConnection(targetSocketId, false);
            }
          });

          this.socket.on('offer', async (data) => {
            await this.handleOffer(data);
          });

          this.socket.on('answer', async (data) => {
            await this.handleAnswer(data);
          });

          this.socket.on('ice-candidate', async (data) => {
            await this.handleIceCandidate(data);
          });
        }

        async initialize() {
          try {
            // Set meeting start time
            this.meetingStartTime = new Date();
            
            // Get meeting name (will be updated when we receive meeting info)
            this.meetingName = `Meeting ${window.location.pathname.split('/').pop()}`;
            
            this.localStream = await navigator.mediaDevices.getUserMedia({
              video: { 
                width: { ideal: 1280 }, 
                height: { ideal: 720 },
                frameRate: { ideal: 30 }
              },
              audio: { 
                echoCancellation: true,
                noiseSuppression: true,
                autoGainControl: true
              }
            });
            
            // Store original microphone track
            this.originalMicrophoneTrack = this.localStream.getAudioTracks()[0];
            
            // Start audio level monitoring
            this.startAudioLevelMonitoring();
            
            // Notify about participant joining
            this.socket.emit('participant-joined', {
                meetingId: window.location.pathname.split('/').pop(),
                userId: window.currentUserId
            });
            
            console.log('Local stream initialized');
            return true;
          } catch (error) {
            console.error('Error accessing media devices:', error);
            return false;
          }
        }

        setReady() {
          this.isReady = true;
          this.socket.emit('participant-ready');
        }

        startAudioLevelMonitoring() {
          if (!this.localStream) return;

          try {
            this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
            const analyser = this.audioContext.createAnalyser();
            const microphone = this.audioContext.createMediaStreamSource(this.localStream);
            const dataArray = new Uint8Array(analyser.frequencyBinCount);

            microphone.connect(analyser);
            analyser.fftSize = 256;

            const checkAudioLevel = () => {
              if (this.audioContext.state === 'suspended') {
                this.audioContext.resume();
              }
              
              analyser.getByteFrequencyData(dataArray);
              const average = dataArray.reduce((a, b) => a + b) / dataArray.length;
              const normalizedLevel = average / 255;

              // Send audio level to server for auto-spotlight
              this.socket.emit('audio-level', { level: normalizedLevel });

              requestAnimationFrame(checkAudioLevel);
            };

            checkAudioLevel();
          } catch (error) {
            console.error('Error setting up audio monitoring:', error);
          }
        }

        async createPeerConnection(remoteSocketId, shouldCreateOffer) {
          try {
            console.log(`Creating peer connection with ${remoteSocketId}, shouldCreateOffer: ${shouldCreateOffer}`);
            
            // Close existing connection if it exists
            if (this.peerConnections.has(remoteSocketId)) {
              this.peerConnections.get(remoteSocketId).close();
              this.peerConnections.delete(remoteSocketId);
            }

            const peerConnection = new RTCPeerConnection(this.configuration);
            this.peerConnections.set(remoteSocketId, peerConnection);

            // Add local stream tracks
            if (this.localStream) {
              this.localStream.getTracks().forEach(track => {
                peerConnection.addTrack(track, this.localStream);
              });
            }

            // Handle remote stream - ENHANCED
            peerConnection.ontrack = (event) => {
              console.log('Received remote track from:', remoteSocketId);
              const [remoteStream] = event.streams;
              this.remoteStreams.set(remoteSocketId, remoteStream);
              
              // Immediately try to update video and setup retry mechanism
              this.updateRemoteVideoWithRetry(remoteSocketId, remoteStream);
              
              // Also trigger a participant re-render to ensure video elements exist
              if (window.hostMeetingInstance) {
                setTimeout(() => {
                  window.hostMeetingInstance.refreshParticipantVideos();
                }, 200);
              }
            };

            // Handle ICE candidates
            peerConnection.onicecandidate = (event) => {
              if (event.candidate) {
                this.socket.emit('ice-candidate', {
                  target: remoteSocketId,
                  candidate: event.candidate
                });
              }
            };

            // Handle connection state changes
            peerConnection.onconnectionstatechange = () => {
              console.log(`Connection state with ${remoteSocketId}:`, peerConnection.connectionState);
              if (peerConnection.connectionState === 'connected') {
                // When connection is established, ensure video is attached
                const stream = this.remoteStreams.get(remoteSocketId);
                if (stream) {
                  this.updateRemoteVideoWithRetry(remoteSocketId, stream);
                }
              } else if (peerConnection.connectionState === 'failed') {
                console.log(`Connection failed with ${remoteSocketId}, attempting restart`);
                peerConnection.restartIce();
              }
            };

            // Create and send offer if we should
            if (shouldCreateOffer) {
              const offer = await peerConnection.createOffer({
                offerToReceiveAudio: true,
                offerToReceiveVideo: true
              });
              await peerConnection.setLocalDescription(offer);
              
              this.socket.emit('offer', {
                target: remoteSocketId,
                offer: offer
              });
            }
          } catch (error) {
            console.error('Error creating peer connection:', error);
          }
        }

        async handleOffer(data) {
          const { offer, sender } = data;
          console.log(`Handling offer from ${sender}`);
          
          try {
            let peerConnection = this.peerConnections.get(sender);
            
            if (!peerConnection) {
              peerConnection = new RTCPeerConnection(this.configuration);
              this.peerConnections.set(sender, peerConnection);

              // Add local stream tracks
              if (this.localStream) {
                this.localStream.getTracks().forEach(track => {
                  peerConnection.addTrack(track, this.localStream);
                });
              }

              // Handle remote stream - ENHANCED
              peerConnection.ontrack = (event) => {
                console.log('Received remote track from:', sender);
                const [remoteStream] = event.streams;
                this.remoteStreams.set(sender, remoteStream);
                
                // Immediately try to update video and setup retry mechanism
                this.updateRemoteVideoWithRetry(sender, remoteStream);
                
                // Also trigger a participant re-render to ensure video elements exist
                if (window.hostMeetingInstance) {
                  setTimeout(() => {
                    window.hostMeetingInstance.refreshParticipantVideos();
                  }, 200);
                }
              };

              // Handle ICE candidates
              peerConnection.onicecandidate = (event) => {
                if (event.candidate) {
                  this.socket.emit('ice-candidate', {
                    target: sender,
                    candidate: event.candidate
                  });
                }
              };

              // Handle connection state changes
              peerConnection.onconnectionstatechange = () => {
                console.log(`Connection state with ${sender}:`, peerConnection.connectionState);
                if (peerConnection.connectionState === 'connected') {
                  // When connection is established, ensure video is attached
                  const stream = this.remoteStreams.get(sender);
                  if (stream) {
                    this.updateRemoteVideoWithRetry(sender, stream);
                  }
                } else if (peerConnection.connectionState === 'failed') {
                  console.log(`Connection failed with ${sender}, attempting restart`);
                  peerConnection.restartIce();
                }
              };
            }

            // Set remote description and create answer
            await peerConnection.setRemoteDescription(new RTCSessionDescription(offer));
            const answer = await peerConnection.createAnswer();
            await peerConnection.setLocalDescription(answer);
            
            this.socket.emit('answer', {
              target: sender,
              answer: answer
            });
          } catch (error) {
            console.error('Error handling offer:', error);
          }
        }

        async handleAnswer(data) {
          const { answer, sender } = data;
          console.log(`Handling answer from ${sender}`);
          
          const peerConnection = this.peerConnections.get(sender);
          
          if (peerConnection && peerConnection.signalingState === 'have-local-offer') {
            try {
              await peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
            } catch (error) {
              console.error('Error handling answer:', error);
            }
          }
        }

        async handleIceCandidate(data) {
          const { candidate, sender } = data;
          const peerConnection = this.peerConnections.get(sender);
          
          if (peerConnection && peerConnection.remoteDescription) {
            try {
              await peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
            } catch (error) {
              console.error('Error handling ICE candidate:', error);
            }
          }
        }

        // ENHANCED: Updated method with retry mechanism
        updateRemoteVideoWithRetry(socketId, stream, maxRetries = 5, currentRetry = 0) {
          const attemptUpdate = () => {
            const videoWrapper = document.querySelector(`[data-socket-id="${socketId}"]`);
            if (videoWrapper) {
              const video = videoWrapper.querySelector('.video-frame');
              if (video) {
                if (video.srcObject !== stream) {
                  video.srcObject = stream;
                  video.play().then(() => {
                    console.log(`Successfully attached stream for ${socketId}`);
                    // Clear retry tracking on success
                    this.streamAttachRetries.delete(socketId);
                  }).catch(e => {
                    console.error('Error playing video:', e);
                    this.retryStreamAttachment(socketId, stream, maxRetries, currentRetry + 1);
                  });
                } else {
                  console.log(`Stream already attached for ${socketId}`);
                  this.streamAttachRetries.delete(socketId);
                }
                return true;
              }
            }
            return false;
          };

          const success = attemptUpdate();
          if (!success) {
            this.retryStreamAttachment(socketId, stream, maxRetries, currentRetry + 1);
          }
        }

        retryStreamAttachment(socketId, stream, maxRetries, currentRetry) {
          if (currentRetry >= maxRetries) {
            console.warn(`Failed to attach stream for ${socketId} after ${maxRetries} attempts`);
            return;
          }

          console.log(`Retrying stream attachment for ${socketId}, attempt ${currentRetry + 1}/${maxRetries}`);
          
          // Store retry info
          this.streamAttachRetries.set(socketId, { 
            stream, 
            retryCount: currentRetry, 
            maxRetries 
          });

          // Progressive delay: 100ms, 200ms, 500ms, 1000ms, 2000ms
          const delays = [100, 200, 500, 1000, 2000];
          const delay = delays[Math.min(currentRetry, delays.length - 1)];

          setTimeout(() => {
            this.updateRemoteVideoWithRetry(socketId, stream, maxRetries, currentRetry);
          }, delay);
        }

        // LEGACY: Keep for backward compatibility but enhance
        updateRemoteVideo(socketId, stream) {
          this.updateRemoteVideoWithRetry(socketId, stream);
        }

        // NEW: Method to refresh all remote video attachments
        refreshAllRemoteVideos() {
          console.log('Refreshing all remote video attachments');
          for (const [socketId, stream] of this.remoteStreams) {
            this.updateRemoteVideoWithRetry(socketId, stream);
          }
        }

        getRemoteStream(socketId) {
          return this.remoteStreams.get(socketId);
        }

        removePeerConnection(socketId) {
          const peerConnection = this.peerConnections.get(socketId);
          if (peerConnection) {
            peerConnection.close();
            this.peerConnections.delete(socketId);
          }
          this.remoteStreams.delete(socketId);
          this.streamAttachRetries.delete(socketId); // Clear retry tracking
        }

        async toggleAudio(enabled) {
          if (this.localStream) {
            const audioTrack = this.localStream.getAudioTracks()[0];
            if (audioTrack) {
              audioTrack.enabled = enabled;
            }
          }
        }

        async toggleVideo(enabled) {
          if (this.localStream) {
            const videoTrack = this.localStream.getVideoTracks()[0];
            if (videoTrack) {
              videoTrack.enabled = enabled;
            }
          }
        }

        async startScreenShare() {
          try {
            console.log('Starting screen share with audio...');
            
            // Request screen share with enhanced audio options
            this.screenStream = await navigator.mediaDevices.getDisplayMedia({
              video: { 
                cursor: 'always',
                displaySurface: 'monitor',
                width: { ideal: 1920 },
                height: { ideal: 1080 },
                frameRate: { ideal: 30 }
              },
              audio: {
                echoCancellation: false,
                noiseSuppression: false,
                autoGainControl: false,
                suppressLocalAudioPlayback: false,
                sampleRate: 48000,
                channelCount: 2
              }
            });

            console.log('Screen stream obtained:', this.screenStream);
            console.log('Video tracks:', this.screenStream.getVideoTracks().length);
            console.log('Audio tracks:', this.screenStream.getAudioTracks().length);

            // Get tracks from screen stream
            const screenVideoTrack = this.screenStream.getVideoTracks()[0];
            const screenAudioTracks = this.screenStream.getAudioTracks();

            // Replace video track in all peer connections
            for (const [socketId, peerConnection] of this.peerConnections) {
              const videoSender = peerConnection.getSenders().find(s => 
                s.track && s.track.kind === 'video'
              );
              
              if (videoSender && screenVideoTrack) {
                console.log(`Replacing video track for peer ${socketId}`);
                await videoSender.replaceTrack(screenVideoTrack);
              }
            }

            // Handle screen share audio
            if (screenAudioTracks.length > 0) {
              console.log('System audio detected, replacing audio tracks...');
              
              // Create a new MediaStream that combines microphone and system audio
              const combinedAudioStream = await this.createCombinedAudioStream(screenAudioTracks[0]);
              
              if (combinedAudioStream) {
                const combinedAudioTrack = combinedAudioStream.getAudioTracks()[0];
                
                // Replace audio track in all peer connections
                for (const [socketId, peerConnection] of this.peerConnections) {
                  const audioSender = peerConnection.getSenders().find(s => 
                    s.track && s.track.kind === 'audio'
                  );
                  
                  if (audioSender && combinedAudioTrack) {
                    console.log(`Replacing audio track for peer ${socketId} with combined audio`);
                    await audioSender.replaceTrack(combinedAudioTrack);
                  } else if (!audioSender && combinedAudioTrack) {
                    console.log(`Adding combined audio track for peer ${socketId}`);
                    peerConnection.addTrack(combinedAudioTrack, combinedAudioStream);
                  }
                }
              } else {
                // Fallback: just use system audio
                const systemAudioTrack = screenAudioTracks[0];
                
                for (const [socketId, peerConnection] of this.peerConnections) {
                  const audioSender = peerConnection.getSenders().find(s => 
                    s.track && s.track.kind === 'audio'
                  );
                  
                  if (audioSender) {
                    console.log(`Replacing audio track for peer ${socketId} with system audio only`);
                    await audioSender.replaceTrack(systemAudioTrack);
                  } else {
                    console.log(`Adding system audio track for peer ${socketId}`);
                    peerConnection.addTrack(systemAudioTrack, this.screenStream);
                  }
                }
              }
            } else {
              console.log('No system audio available for screen share');
              // Keep using microphone audio
            }

            // Update local video to show screen share
            const localVideo = document.querySelector(`[data-socket-id="${this.socket.id}"] .video-frame`);
            if (localVideo) {
              localVideo.srcObject = this.screenStream;
            }

            // Add screen share label
            const localWrapper = document.querySelector(`[data-socket-id="${this.socket.id}"]`);
            if (localWrapper) {
              let label = localWrapper.querySelector('.video-label');
              if (!label) {
                label = document.createElement('div');
                label.className = 'video-label';
                localWrapper.appendChild(label);
              }
              label.innerHTML = '<i class="fas fa-desktop"></i> Screen Share' + 
                (screenAudioTracks.length > 0 ? ' (with audio)' : '');
            }

            // Handle screen share end
            screenVideoTrack.onended = () => {
              console.log('Screen share ended');
              this.stopScreenShare();
            };

            // Handle audio track end if present
            if (screenAudioTracks.length > 0) {
              screenAudioTracks[0].onended = () => {
                console.log('Screen share audio ended');
              };
            }

            this.isScreenSharing = true;
            console.log('Screen share started successfully');
            
          } catch (error) {
            console.error('Error starting screen share:', error);
            throw error;
          }
        }

        async createCombinedAudioStream(systemAudioTrack) {
          try {
            if (!this.audioContext) {
              this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
            }

            // Resume audio context if suspended
            if (this.audioContext.state === 'suspended') {
              await this.audioContext.resume();
            }

            // Create audio sources
            const systemAudioSource = this.audioContext.createMediaStreamSource(
              new MediaStream([systemAudioTrack])
            );
            
            let microphoneSource = null;
            if (this.originalMicrophoneTrack && this.originalMicrophoneTrack.enabled) {
              microphoneSource = this.audioContext.createMediaStreamSource(
                new MediaStream([this.originalMicrophoneTrack])
              );
            }

            // Create gain nodes for volume control
            const systemGain = this.audioContext.createGain();
            const micGain = this.audioContext.createGain();
            const outputGain = this.audioContext.createGain();

            // Set gain levels
            systemGain.gain.value = 1.0; // Full system audio
            micGain.gain.value = 0.7;    // Slightly reduced microphone
            outputGain.gain.value = 1.0;

            // Create destination for combined audio
            const destination = this.audioContext.createMediaStreamDestination();

            // Connect audio graph
            systemAudioSource.connect(systemGain);
            systemGain.connect(outputGain);

            if (microphoneSource) {
              microphoneSource.connect(micGain);
              micGain.connect(outputGain);
            }

            outputGain.connect(destination);

            console.log('Combined audio stream created successfully');
            return destination.stream;

          } catch (error) {
            console.error('Error creating combined audio stream:', error);
            return null;
          }
        }

        async stopScreenShare() {
          console.log('Stopping screen share...');
          
          if (this.screenStream) {
            this.screenStream.getTracks().forEach(track => {
              console.log(`Stopping track: ${track.kind}`);
              track.stop();
            });
            this.screenStream = null;
          }

          // Replace back to camera video and microphone audio
          if (this.localStream) {
            const videoTrack = this.localStream.getVideoTracks()[0];
            const audioTrack = this.originalMicrophoneTrack || this.localStream.getAudioTracks()[0];
            
            for (const [socketId, peerConnection] of this.peerConnections) {
              // Replace video track back to camera
              const videoSender = peerConnection.getSenders().find(s => 
                s.track && s.track.kind === 'video'
              );
              
              if (videoSender && videoTrack) {
                console.log(`Restoring camera video for peer ${socketId}`);
                await videoSender.replaceTrack(videoTrack);
              }

              // Replace audio track back to microphone
              const audioSender = peerConnection.getSenders().find(s => 
                s.track && s.track.kind === 'audio'
              );
              
              if (audioSender && audioTrack) {
                console.log(`Restoring microphone audio for peer ${socketId}`);
                await audioSender.replaceTrack(audioTrack);
              }
            }

            // Update local video back to camera
            const localVideo = document.querySelector(`[data-socket-id="${this.socket.id}"] .video-frame`);
            if (localVideo) {
              localVideo.srcObject = this.localStream;
            }

            // Remove screen share label
            const localWrapper = document.querySelector(`[data-socket-id="${this.socket.id}"]`);
            if (localWrapper) {
              const label = localWrapper.querySelector('.video-label');
              if (label) {
                label.remove();
              }
            }
          }

          this.isScreenSharing = false;
          console.log('Screen share stopped successfully');
        }

        leaveMeeting() {
          try {
            // Notify about participant leaving
            if (this.socket && window.currentUserId) {
              this.socket.emit('participant-left', {
                  meetingId: window.location.pathname.split('/').pop(),
                  userId: window.currentUserId
              });
            }
            
            // Stop local stream
            if (this.localStream) {
              this.localStream.getTracks().forEach(track => track.stop());
            }
            
            // Stop screen stream
            if (this.screenStream) {
              this.screenStream.getTracks().forEach(track => track.stop());
            }
            
            // Close all peer connections
            this.peerConnections.forEach(pc => pc.close());
            this.peerConnections.clear();
            this.remoteStreams.clear();
            
            // Close audio context
            if (this.audioContext) {
              this.audioContext.close();
            }
            
            console.log('WebRTC cleanup completed');
          } catch (error) {
            console.error('Error during WebRTC cleanup:', error);
          }
        }
      }

      class ParticipantMeeting {
        constructor() {
          this.socket = io();
          window.socket = this.socket; // Make socket globally available for stats
          this.meetingId = window.location.pathname.split('/').pop();
          this.userName = '';
          this.userId = null;
          this.isHost = false;
          this.isCoHost = false;
          this.participants = new Map();
          this.currentView = 'sidebar';
          this.spotlightedParticipant = null;
          this.pinnedParticipant = null;
          this.webrtc = new WebRTCManager(this.socket);
          this.participantsPanelOpen = false;
          this.searchTerm = '';
          this.reactionManager = null;
          
          // Grid pagination properties
          this.currentSet = 0;
          this.videosPerSet = 15;
          this.totalSets = 0;
          
          this.init().then(() => {
            // Store global references after initialization
            window.hostMeetingInstance = this;
            window.myName = this.userName;
            console.log('Host meeting initialized. Host name:', window.myName);
          });
        }

        async init() {
          await this.getUserName();
          this.setupSocketListeners();
          this.setupEventListeners();
          this.updateTime();
          this.joinMeeting();
          
          // Initialize WebRTC and show local video immediately
          const initialized = await this.webrtc.initialize();
          if (initialized) {
            this.showLocalVideo();
            // Set ready after a short delay to ensure everything is set up
            setTimeout(() => {
              this.webrtc.setReady();
            }, 1000);
            this.renderParticipants();
            
            // NEW: Start periodic video refresh to ensure streams are attached
            this.startVideoRefreshMonitor();
          }

          // Initialize Reaction Manager
          this.reactionManager = new ReactionManager(this.socket);
        }

        // NEW: Monitor and refresh video attachments periodically
        startVideoRefreshMonitor() {
          // Check every 3 seconds for missing video attachments
          setInterval(() => {
            this.checkAndRefreshMissingVideos();
          }, 3000);
        }

        // NEW: Check for video elements without streams and refresh them
        checkAndRefreshMissingVideos() {
          const videoWrappers = document.querySelectorAll('.video-wrapper[data-socket-id]');
          let refreshNeeded = false;

          videoWrappers.forEach(wrapper => {
            const socketId = wrapper.dataset.socketId;
            if (socketId !== this.socket.id) { // Skip local video
              const video = wrapper.querySelector('.video-frame');
              const remoteStream = this.webrtc.getRemoteStream(socketId);
              
              if (video && remoteStream && !video.srcObject) {
                console.log(`Found missing video stream for ${socketId}, refreshing...`);
                this.webrtc.updateRemoteVideoWithRetry(socketId, remoteStream);
                refreshNeeded = true;
              }
            }
          });

          if (refreshNeeded) {
            console.log('Refreshed missing video streams');
          }
        }

        // NEW: Method to refresh participant videos (called by WebRTC manager)
        refreshParticipantVideos() {
          // Ensure all video elements exist and are properly attached
          setTimeout(() => {
            this.attachStreamsToExistingVideos();
          }, 100);
        }

        // NEW: Attach streams to existing video elements
        attachStreamsToExistingVideos() {
          const videoWrappers = document.querySelectorAll('.video-wrapper[data-socket-id]');
          
          videoWrappers.forEach(wrapper => {
            const socketId = wrapper.dataset.socketId;
            const video = wrapper.querySelector('.video-frame');
            
            if (video && socketId !== this.socket.id) {
              const remoteStream = this.webrtc.getRemoteStream(socketId);
              if (remoteStream && video.srcObject !== remoteStream) {
                video.srcObject = remoteStream;
                video.play().catch(e => console.error('Error playing refreshed video:', e));
              }
            }
          });
        }

        showLocalVideo() {
          // Create local video immediately
          this.participants.set(this.socket.id, {
            socketId: this.socket.id,
            name: this.userName,
            isHost: false,
            isCoHost: false,
            isMuted: false,
            isCameraOff: false,
            isSpotlighted: false,
            isScreenSharing: false,
            handRaised: false
          });
          this.renderParticipants();
          this.renderParticipantsList();
        }

        async getUserName() {
          try {
            const response = await fetch('/api/user');
            const data = await response.json();
            if (data.user) {
              this.userName = data.user.name;
              this.userId = data.user.id;
              window.currentUserId = this.userId; // Set for stats tracking
            } else {
              window.location.href = '/login';
            }
          } catch (error) {
            console.error('Error fetching user data:', error);
            window.location.href = '/login';
          }
        }

        setupSocketListeners() {
          this.socket.on('joined-meeting', (data) => {
            console.log('Joined meeting as participant:', data);
            this.updateParticipants(data.participants);
            this.spotlightedParticipant = data.spotlightedParticipant;
            this.updateMeetingTitle();
            this.updateRaisedHands(data.raisedHands);
          });

          this.socket.on('participant-joined', (data) => {
            console.log('Participant joined:', data);
            this.updateParticipants(data.participants);
            this.showToast(`${data.participant.name} joined the meeting`);
            
            // NEW: Ensure video elements are ready for new participant
            setTimeout(() => {
              this.refreshParticipantVideos();
            }, 500);
          });

          this.socket.on('participant-left', (data) => {
            console.log('Participant left:', data);
            this.removeParticipantVideo(data.socketId);
            this.updateParticipants(data.participants);
            this.showToast(`${data.participantName} left the meeting`);
            
            // Clean up WebRTC connection
            this.webrtc.removePeerConnection(data.socketId);
          });

          this.socket.on('participant-spotlighted', (data) => {
            console.log('Participant spotlighted:', data);
            this.handleSpotlightChange(data.spotlightedParticipant);
            this.updateParticipants(data.participants);
          });

          this.socket.on('spotlight-removed', (data) => {
            console.log('Spotlight removed:', data);
            this.handleSpotlightRemoved();
            this.updateParticipants(data.participants);
          });

          this.socket.on('participant-pinned', (data) => {
            console.log('Participant pinned:', data);
            this.handlePinChange(data.pinnedParticipant);
          });

          this.socket.on('force-mute', (data) => {
            console.log('Force muted:', data);
            this.handleForceMute(data.isMuted);
          });

          this.socket.on('made-cohost', () => {
            console.log('Made co-host');
            this.isCoHost = true;
            this.showToast('You are now a co-host!');
            this.renderParticipants();
            this.renderParticipantsList();
          });

          this.socket.on('kicked-from-meeting', () => {
            console.log('Kicked from meeting');
            document.getElementById('kickedModal').style.display = 'flex';
          });

          this.socket.on('meeting-ended', () => {
            console.log('Meeting ended');
            document.getElementById('meetingEndedModal').style.display = 'flex';
          });

          this.socket.on('participant-muted', (data) => {
            console.log('Participant muted:', data);
            this.updateParticipantAudio(data.targetSocketId, data.isMuted);
            this.updateParticipants(data.participants);
          });

          this.socket.on('meeting-error', (data) => {
            console.error('Meeting error:', data);
            this.showToast(data.message, 'error');
            setTimeout(() => {
              window.location.href = '/dashboard';
            }, 3000);
          });

          this.socket.on('action-error', (data) => {
            console.error('Action error:', data);
            this.showToast(data.message, 'error');
          });

          // Hand raised events
          this.socket.on('hand-raised', (data) => {
            this.updateRaisedHands(data.raisedHands);
            if (this.reactionManager) {
              this.reactionManager.updateHandRaised(data.socketId, data.participantName, true);
            }
          });

          this.socket.on('hand-lowered', (data) => {
            this.updateRaisedHands(data.raisedHands);
            if (this.reactionManager) {
              this.reactionManager.updateHandRaised(data.socketId, data.participantName, false);
            }
          });
        }

        setupEventListeners() {
          // Participants panel toggle
          document.getElementById('memberToggleBtn').addEventListener('click', () => {
            this.toggleParticipantsPanel();
          });

          document.getElementById('closeParticipants').addEventListener('click', () => {
            this.closeParticipantsPanel();
          });

          // Search functionality
          document.getElementById('participantSearch').addEventListener('input', (e) => {
            this.searchTerm = e.target.value.toLowerCase();
            this.renderParticipantsList();
          });

          // View toggle - ENHANCED to also refresh videos
          document.getElementById('viewToggle').addEventListener('click', () => {
            this.toggleView();
            // Ensure all videos are properly attached after view change
            setTimeout(() => {
              this.refreshParticipantVideos();
              this.webrtc.refreshAllRemoteVideos();
            }, 200);
          });

          // Mic toggle
          document.getElementById('micBtn').addEventListener('click', (e) => {
            this.toggleMic(e.currentTarget);
          });

          // Camera toggle
          document.getElementById('cameraBtn').addEventListener('click', (e) => {
            this.toggleCamera(e.currentTarget);
          });

          // Screen share toggle
          document.getElementById('screenShareBtn').addEventListener('click', (e) => {
            this.toggleScreenShare(e.currentTarget);
          });

          // Leave call
          document.getElementById('leaveCallBtn').addEventListener('click', () => {
            this.leaveMeeting();
          });

          // Close participants panel when clicking outside
          document.addEventListener('click', (e) => {
            if (this.participantsPanelOpen && 
                !document.getElementById('participantsPanel').contains(e.target) &&
                !document.getElementById('memberToggleBtn').contains(e.target)) {
              this.closeParticipantsPanel();
            }
          });
        }

        updateRaisedHands(raisedHands) {
          if (this.reactionManager) {
            this.reactionManager.raisedHands.clear();
            raisedHands.forEach(socketId => {
              this.reactionManager.raisedHands.add(socketId);
            });
            this.reactionManager.updateParticipantsDisplay();
          }
        }

        toggleParticipantsPanel() {
          if (this.participantsPanelOpen) {
            this.closeParticipantsPanel();
          } else {
            this.openParticipantsPanel();
          }
        }

        openParticipantsPanel() {
          this.participantsPanelOpen = true;
          document.getElementById('participantsPanel').classList.add('open');
          document.getElementById('videoContainer').classList.add('participants-open');
          this.renderParticipantsList();
          const chatBar = document.getElementById("chatBar");
          chatBar.classList.remove("open");
        }

        closeParticipantsPanel() {
          this.participantsPanelOpen = false;
          document.getElementById('participantsPanel').classList.remove('open');
          document.getElementById('videoContainer').classList.remove('participants-open');
        }

        renderParticipantsList() {
          const participantsList = document.getElementById('participantsList');
          const participantsPanelCount = document.getElementById('participantsPanelCount');
          
          participantsList.innerHTML = '';
          
          const filteredParticipants = Array.from(this.participants.values()).filter(participant => 
            participant.name.toLowerCase().includes(this.searchTerm)
          );

          participantsPanelCount.textContent = filteredParticipants.length;

          filteredParticipants.forEach(participant => {
            const participantItem = this.createParticipantItem(participant);
            participantsList.appendChild(participantItem);
          });

          // Update reaction manager if available
          if (this.reactionManager) {
            this.reactionManager.onParticipantsUpdated();
          }
        }

        createParticipantItem(participant) {
          const item = document.createElement('div');
          item.className = 'participant-item';
          item.dataset.socketId = participant.socketId;

          const initials = participant.name.split(' ').map(n => n[0]).join('').toUpperCase();
          
          let roleText = 'Participant';
          let roleClass = 'participant';
          if (participant.isHost) {
            roleText = 'Host';
            roleClass = 'host';
          } else if (participant.isCoHost) {
            roleText = 'Co-Host';
            roleClass = 'cohost';
          }

          const statusIcons = [];
          if (participant.isMuted) {
            statusIcons.push('<div class="status-icon muted"><i class="fas fa-microphone-slash"></i></div>');
          }
          if (participant.isCameraOff) {
            statusIcons.push('<div class="status-icon camera-off"><i class="fas fa-video-slash"></i></div>');
          }

          const dropdownOptions = this.getParticipantDropdownOptions(participant);

          item.innerHTML = `
            <div class="participant-avatar">${initials}</div>
            <div class="participant-info">
              <div class="participant-name-section">${participant.name}</div>
              <div class="participant-role">
                <span class="role-badge ${roleClass}">${roleText}</span>
                ${participant.isSpotlighted ? '<i class="fas fa-star" style="color: #fbbf24; margin-left: 4px;"></i>' : ''}
                ${this.pinnedParticipant === participant.socketId ? '<i class="fas fa-thumbtack" style="color: #10b981; margin-left: 4px;"></i>' : ''}
              </div>
            </div>
            <div class="participant-status">
              ${statusIcons.join('')}
            </div>
            <div class="participant-actions">
              <button class="participant-menu-btn" data-participant-id="${participant.socketId}">
                <i class="fas fa-ellipsis-v"></i>
              </button>
              <div class="participant-dropdown" id="dropdown-${participant.socketId}">
                ${dropdownOptions}
              </div>
            </div>
          `;

          // Bind events
          const menuBtn = item.querySelector('.participant-menu-btn');
          const dropdown = item.querySelector('.participant-dropdown');

          menuBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            // Close all other dropdowns
            document.querySelectorAll('.participant-dropdown').forEach(d => {
              if (d !== dropdown) d.classList.remove('show');
            });
            dropdown.classList.toggle('show');
          });

          // Bind dropdown actions
          const dropdownButtons = dropdown.querySelectorAll('button');
          dropdownButtons.forEach(button => {
            button.addEventListener('click', (e) => {
              e.stopPropagation();
              const action = button.dataset.action;
              this.handleParticipantAction(action, participant.socketId);
              dropdown.classList.remove('show');
            });
          });

          return item;
        }

        getParticipantDropdownOptions(participant) {
          let options = [];
          
          // Pin option (available to all participants)
          if (this.pinnedParticipant === participant.socketId) {
            options.push('<button data-action="unpin"><i class="fas fa-thumbtack"></i> Unpin</button>');
          } else {
            options.push('<button data-action="pin"><i class="fas fa-thumbtack"></i> Pin</button>');
          }
          
          // Co-host and host actions
          if (this.isCoHost && !participant.isHost) {
            if (participant.isSpotlighted) {
              options.push('<button data-action="remove-spotlight"><i class="fas fa-star-half-alt"></i> Remove Spotlight</button>');
            } else {
              options.push('<button data-action="spotlight"><i class="fas fa-star"></i> Spotlight</button>');
            }
            
            options.push(`<button data-action="mute"><i class="fas fa-microphone-slash"></i> ${participant.isMuted ? 'Unmute' : 'Mute'}</button>`);
          }
          
          return options.join('');
        }

        joinMeeting() {
          this.socket.emit('join-meeting', {
            meetingId: this.meetingId,
            participantName: this.userName,
            userId: this.userId
          });
        }

        updateParticipants(participants) {
          // Keep local participant if not in server list
          const localParticipant = this.participants.get(this.socket.id);
          
          this.participants.clear();
          participants.forEach(p => {
            this.participants.set(p.socketId, p);
            if (p.socketId === this.socket.id) {
              this.isCoHost = p.isCoHost;
            }
          });

          // Ensure local participant is always present
          if (localParticipant && !this.participants.has(this.socket.id)) {
            this.participants.set(this.socket.id, localParticipant);
          }

          this.renderParticipants();
          this.updateParticipantCount();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }

          // NEW: Ensure videos are attached after participant updates
          setTimeout(() => {
            this.refreshParticipantVideos();
          }, 200);
        }

        calculateGridPagination() {
          const totalParticipants = this.participants.size;
          this.totalSets = Math.ceil(totalParticipants / this.videosPerSet);
          
          // Ensure current set is within bounds
          if (this.currentSet >= this.totalSets) {
            this.currentSet = Math.max(0, this.totalSets - 1);
          }
        }

        getCurrentSetParticipants() {
          const participantArray = Array.from(this.participants.values());
          const startIndex = this.currentSet * this.videosPerSet;
          const endIndex = startIndex + this.videosPerSet;
          return participantArray.slice(startIndex, endIndex);
        }

        updateGridSizeClass() {
          const videoContainer = document.getElementById('videoContainer');
          const participantCount = this.participants.size;
          
          // Remove all existing size classes
          videoContainer.classList.remove(
            'participants-2', 'participants-3', 'participants-4', 
            'participants-5', 'participants-6'
          );
          
          // Add appropriate size class
          if (participantCount === 2) {
            videoContainer.classList.add('participants-2');
          } else if (participantCount === 3) {
            videoContainer.classList.add('participants-3');
          } else if (participantCount === 4) {
            videoContainer.classList.add('participants-4');
          } else if (participantCount === 5) {
            videoContainer.classList.add('participants-5');
          } else if (participantCount === 6) {
            videoContainer.classList.add('participants-6');
          }
          else if (participantCount === 7) {
            videoContainer.classList.add('participants-7');
          }
             else if (participantCount === 8) {
            videoContainer.classList.remove('participants-7');
               videoContainer.classList.add('participants-8');
          }

            else if (participantCount === 9) {
            videoContainer.classList.remove('participants-8');
               videoContainer.classList.add('participants-9');
          }
           else if (participantCount === 10) {
            videoContainer.classList.remove('participants-9');
               videoContainer.classList.add('participants-10');
          }
            else if (participantCount === 11) {
            videoContainer.classList.remove('participants-10');
               videoContainer.classList.add('participants-11');
          }
           else if (participantCount === 12) {
            videoContainer.classList.remove('participants-11');
               videoContainer.classList.add('participants-12');
          }
           else if (participantCount === 13) {
            videoContainer.classList.remove('participants-12');
               videoContainer.classList.add('participants-13');
          }
            else if (participantCount === 14) {
            videoContainer.classList.remove('participants-13');
               videoContainer.classList.add('participants-14');
          }
          else if (participantCount === 15) {
            videoContainer.classList.remove('participants-14');
               videoContainer.classList.add('participants-15');
          }
          // For 7+ participants, no special class is added (uses default grid)
        }

        createGridNavigation() {
          const videoContainer = document.getElementById('videoContainer');
          let navigation = videoContainer.querySelector('.grid-navigation');
          
          if (!navigation) {
            navigation = document.createElement('div');
            navigation.className = 'grid-navigation';
            videoContainer.appendChild(navigation);
          }
          
          const startParticipant = this.currentSet * this.videosPerSet + 1;
          const endParticipant = Math.min((this.currentSet + 1) * this.videosPerSet, this.participants.size);
          
          navigation.innerHTML = `
            <button class="grid-nav-btn" id="prevSetBtn" ${this.currentSet === 0 ? 'disabled' : ''}>
              <i class="fas fa-chevron-left"></i>
              Previous
            </button>
            <div class="grid-nav-info">
              ${startParticipant}-${endParticipant} of ${this.participants.size}
            </div>
            <button class="grid-nav-btn" id="nextSetBtn" ${this.currentSet >= this.totalSets - 1 ? 'disabled' : ''}>
              Next
              <i class="fas fa-chevron-right"></i>
            </button>
          `;
          
          // Bind navigation events
          const prevBtn = navigation.querySelector('#prevSetBtn');
          const nextBtn = navigation.querySelector('#nextSetBtn');
          
          prevBtn.addEventListener('click', () => {
            if (this.currentSet > 0) {
              this.currentSet--;
              this.renderParticipants();
            }
          });
          
          nextBtn.addEventListener('click', () => {
            if (this.currentSet < this.totalSets - 1) {
              this.currentSet++;
              this.renderParticipants();
            }
          });
        }

        renderParticipants() {
          const mainVideoSection = document.getElementById('mainVideoSection');
          const secondaryVideosSection = document.getElementById('secondaryVideosSection');
          
          // Clear existing videos
          mainVideoSection.innerHTML = '';
          secondaryVideosSection.innerHTML = '';

          if (this.currentView === 'grid') {
            this.calculateGridPagination();
            this.updateGridSizeClass();
            
            // Get participants for current set
            const currentSetParticipants = this.getCurrentSetParticipants();
            
            currentSetParticipants.forEach(participant => {
              const videoWrapper = this.createVideoWrapper(participant);
              secondaryVideosSection.appendChild(videoWrapper);
            });
            
            // Create navigation if we have multiple sets
            if (this.totalSets > 1) {
              this.createGridNavigation();
            }
          } else {
            // Sidebar view - show all participants
            const participantArray = Array.from(this.participants.values());
            
            participantArray.forEach(participant => {
              const videoWrapper = this.createVideoWrapper(participant);
              
              // Check if this participant should be in main view
              const shouldBeMain = (this.spotlightedParticipant === participant.socketId || 
                                   this.pinnedParticipant === participant.socketId);
              
              if (shouldBeMain && /Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent)) {
                videoWrapper.classList.add('main-video');
                videoWrapper.setAttribute('data-main-video', 'true');
                
                secondaryVideosSection.appendChild(videoWrapper);
                
              } 
              else if (shouldBeMain && /Windows|Macintosh|Linux/i.test(navigator.userAgent)){
                    videoWrapper.classList.add('main-video');
                videoWrapper.setAttribute('data-main-video', 'true');
                mainVideoSection.appendChild(videoWrapper);
              }
              else {
                secondaryVideosSection.appendChild(videoWrapper);
              }
            });
          }

          // NEW: Ensure all streams are attached after rendering
          setTimeout(() => {
            this.attachStreamsToExistingVideos();
          }, 100);
        }

        createVideoWrapper(participant) {
          const wrapper = document.createElement('div');
          wrapper.className = 'video-wrapper';
          wrapper.dataset.socketId = participant.socketId;
          
          if (participant.isSpotlighted || this.pinnedParticipant === participant.socketId) {
            wrapper.setAttribute('data-main-video', 'true');
          }

          const dropdownOptions = this.getDropdownOptions(participant);
          
          wrapper.innerHTML = `
            <video class="video-frame" autoplay playsinline ${participant.socketId === this.socket.id ? 'muted' : ''}></video>
            <div class="video-controls">
              <button class="menu-dots">⋮</button>
              <div class="dropdown-menu">
                ${dropdownOptions}
              </div>
            </div>
            <div class="participant-name">${participant.name}${participant.isHost ? ' (Host)' : ''}${participant.isCoHost ? ' (Co-Host)'  : ''}</div>
            ${participant.isSpotlighted ? '<div style="display: none" class="spotlight-badge"><i class="fas fa-star"></i></div>' : ''}
            ${this.pinnedParticipant === participant.socketId ? '<div class="pin-badge"><i class="fas fa-thumbtack"></i></div>' : ''}
            ${participant.isMuted ? '<div class="audio-indicator"><i class="fas fa-microphone-slash"></i></div>' : ''}
          `;

          this.bindVideoWrapperEvents(wrapper, participant);
          
          // ENHANCED: Attach video stream with better error handling
          setTimeout(() => {
            const video = wrapper.querySelector('.video-frame');
            if (participant.socketId === this.socket.id) {
              // Local video
              if (this.webrtc.isScreenSharing && this.webrtc.screenStream) {
                video.srcObject = this.webrtc.screenStream;
              } else if (this.webrtc.localStream) {
                video.srcObject = this.webrtc.localStream;
              }
              video.play().catch(e => console.error('Error playing local video:', e));
            } else {
              // Remote video - use enhanced method
              const remoteStream = this.webrtc.getRemoteStream(participant.socketId);
              if (remoteStream) {
                video.srcObject = remoteStream;
                video.play().catch(e => {
                  console.error('Error playing remote video:', e);
                  // Retry with the enhanced method
                  this.webrtc.updateRemoteVideoWithRetry(participant.socketId, remoteStream);
                });
              } else {
                // Stream not available yet, it will be attached when received
                console.log(`Remote stream not yet available for ${participant.socketId}`);
              }
            }
          }, 50); // Reduced timeout for faster attachment
          
          return wrapper;
        }

        getDropdownOptions(participant) {
          let options = [];
          
          // Pin option (available to all participants)
          if (this.pinnedParticipant === participant.socketId) {
            options.push('<button data-action="unpin">Unpin</button>');
          } else {
            options.push('<button data-action="pin">Pin</button>');
          }
          
          // Co-host and host actions
          if (this.isCoHost && !participant.isHost) {
            if (participant.isSpotlighted) {
              options.push('<button data-action="remove-spotlight">Remove Spotlight</button>');
            } else {
              options.push('<button data-action="spotlight">Spotlight</button>');
            }
            
            options.push(`<button data-action="mute">${participant.isMuted ? 'Unmute' : 'Mute'} Participant</button>`);
          }
          
          return options.join('');
        }

        bindVideoWrapperEvents(wrapper, participant) {
          // Double click to pin
          wrapper.addEventListener('dblclick', () => {
            this.pinParticipant(participant.socketId);
          });

          // Dropdown menu actions
          const dropdownButtons = wrapper.querySelectorAll('.dropdown-menu button');
          dropdownButtons.forEach(button => {
            button.addEventListener('click', (e) => {
              e.stopPropagation();
              const action = button.dataset.action;
              this.handleParticipantAction(action, participant.socketId);
            });
          });
        }

        handleParticipantAction(action, socketId) {
          switch(action) {
            case 'pin':
              this.pinParticipant(socketId);
              break;
            case 'unpin':
              this.unpinParticipant();
              break;
            case 'spotlight':
              this.spotlightParticipant(socketId);
              break;
            case 'remove-spotlight':
              this.removeSpotlight();
              break;
            case 'mute':
              this.muteParticipant(socketId);
              break;
          }
        }

        pinParticipant(socketId) {
          this.pinnedParticipant = socketId;
          this.socket.emit('pin-participant', { targetSocketId: socketId });
          this.renderParticipants();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }
          
          const participant = this.participants.get(socketId);
          this.showToast(`Pinned ${participant?.name || 'participant'}`);
        }

        unpinParticipant() {
          this.pinnedParticipant = null;
          this.renderParticipants();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }
          this.showToast('Unpinned participant');
        }

        spotlightParticipant(socketId) {
          this.socket.emit('spotlight-participant', { targetSocketId: socketId });
        }

        removeSpotlight() {
          this.socket.emit('remove-spotlight');
        }

        muteParticipant(socketId) {
          this.socket.emit('mute-participant', { targetSocketId: socketId });
        }

        handleSpotlightChange(spotlightedSocketId) {
          this.spotlightedParticipant = spotlightedSocketId;
          this.renderParticipants();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }
        }

        handleSpotlightRemoved() {
          this.spotlightedParticipant = null;
          this.renderParticipants();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }
        }

        handlePinChange(pinnedSocketId) {
          this.pinnedParticipant = pinnedSocketId;
          this.renderParticipants();
          if (this.participantsPanelOpen) {
            this.renderParticipantsList();
          }
        }

        handleForceMute(isMuted) {
          const micBtn = document.getElementById('micBtn');
          micBtn.setAttribute('data-active', !isMuted);
          
          const icon = micBtn.querySelector('i');
          icon.className = isMuted ? 'fas fa-microphone-slash' : 'fas fa-microphone';
          
          this.showToast(isMuted ? 'You have been muted by the host' : 'You have been unmuted by the host');
        }

        removeParticipantVideo(socketId) {
          const wrapper = document.querySelector(`[data-socket-id="${socketId}"]`);
          if (wrapper) {
            wrapper.style.transition = 'all 0.3s ease';
            wrapper.style.opacity = '0';
            wrapper.style.transform = 'scale(0.8)';
            setTimeout(() => wrapper.remove(), 300);
          }
        }

        updateParticipantAudio(socketId, isMuted) {
          const wrapper = document.querySelector(`[data-socket-id="${socketId}"]`);
          if (wrapper) {
            let audioIndicator = wrapper.querySelector('.audio-indicator');
            if (isMuted && !audioIndicator) {
              audioIndicator = document.createElement('div');
              audioIndicator.className = 'audio-indicator';
              audioIndicator.innerHTML = '<i class="fas fa-microphone-slash"></i>';
              wrapper.appendChild(audioIndicator);
            } else if (!isMuted && audioIndicator) {
              audioIndicator.remove();
            }
          }
        }

        toggleView() {
          const videoContainer = document.getElementById('videoContainer');
          const viewToggleIcon = document.getElementById('viewToggleIcon');
          const viewToggleText = document.getElementById('viewToggleText');
          
          if (this.currentView === 'sidebar') {
            this.currentView = 'grid';
            this.currentSet = 0; // Reset to first set when switching to grid
            videoContainer.classList.remove('sidebar-view');
            videoContainer.classList.add('grid-view');
            viewToggleIcon.className = 'fas fa-columns';
            viewToggleText.textContent = 'Sidebar View';
          } else {
            this.currentView = 'sidebar';
            videoContainer.classList.remove('grid-view');
            videoContainer.classList.add('sidebar-view');
            viewToggleIcon.className = 'fas fa-th';
            viewToggleText.textContent = 'Grid View';
            
            // Remove navigation when switching to sidebar
            const navigation = videoContainer.querySelector('.grid-navigation');
            if (navigation) {
              navigation.remove();
            }
          }
          
          this.renderParticipants();
        }

        async toggleMic(button) {
          const isActive = button.getAttribute('data-active') === 'true';
          button.setAttribute('data-active', !isActive);
          
          const icon = button.querySelector('i');
          icon.className = isActive ? 'fas fa-microphone-slash' : 'fas fa-microphone';
          
          await this.webrtc.toggleAudio(!isActive);
          this.socket.emit('toggle-mic', { isMuted: isActive });
        }

        async toggleCamera(button) {
          const isActive = button.getAttribute('data-active') === 'true';
          button.setAttribute('data-active', !isActive);
          
          const icon = button.querySelector('i');
          icon.className = isActive ? 'fas fa-video-slash' : 'fas fa-video';
          
          await this.webrtc.toggleVideo(!isActive);
          this.socket.emit('toggle-camera', { isCameraOff: isActive });
        }

        async toggleScreenShare(button) {
          const isActive = button.getAttribute('data-active') === 'true';
          
          if (isActive) {
            await this.webrtc.stopScreenShare();
            button.setAttribute('data-active', 'false');
            this.socket.emit('stop-screen-share');
          } else {
            try {
              await this.webrtc.startScreenShare();
              button.setAttribute('data-active', 'true');
              this.socket.emit('start-screen-share', { streamId: 'screen' });
            } catch (error) {
              console.error('Failed to start screen share:', error);
              this.showToast('Failed to start screen sharing', 'error');
            }
          }
        }

        updateParticipantCount() {
          const count = this.participants.size;
          document.getElementById('participantCount').textContent = count;
        }

        updateMeetingTitle() {
          document.getElementById('meetingTitle').textContent = `Meeting ${this.meetingId}`;
        }

        updateTime() {
          const timeElement = document.getElementById('meetingTime');
          const now = new Date();
          const timeString = now.toLocaleTimeString('en-US', { 
            hour: '2-digit', 
            minute: '2-digit',
            hour12: true 
          });
          timeElement.textContent = timeString;
          
          setTimeout(() => this.updateTime(), 60000);
        }

        showToast(message, type = 'success') {
          const toast = document.createElement('div');
          toast.className = `toast ${type === 'error' ? 'error' : type === 'info' ? 'info' : ''}`;
          toast.textContent = message;
          
          document.body.appendChild(toast);
          
          setTimeout(() => toast.classList.add('show'), 100);
          setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
          }, 3000);
        }

        leaveMeeting() {
          if (confirm('Are you sure you want to leave the meeting?')) {
            // Get meeting details before leaving
            const meetingName = document.getElementById('meetingTitle')?.textContent || 
                               document.querySelector('.meeting-title')?.textContent || 
                               document.querySelector('h1')?.textContent || 
                               'Meeting';
            
            const joinTime = meetingJoinTime || new Date();
            const leaveTime = new Date();
            const duration = Math.round((leaveTime - joinTime) / (1000 * 60)); // Duration in minutes
            
            // Get current user info
            const userId = currentUser?.id;
            
            // Emit user left event for activity tracking
            if (socket && userId) {
                socket.emit('user-left-meeting', {
                    meetingId: meetingId,
                    meetingName: meetingName,
                    userId: userId,
                    duration: duration,
                    joinTime: joinTime,
                    leaveTime: leaveTime
                });
            }
            
            this.webrtc.leaveMeeting();
            this.socket.disconnect();
            window.location.href = '/dashboard';
          }
        }
      }

      // Close dropdowns when clicking outside
      document.addEventListener('click', () => {
        document.querySelectorAll('.participant-dropdown').forEach(dropdown => {
          dropdown.classList.remove('show');
        });
      });

      // Initialize the participant meeting
      document.addEventListener('DOMContentLoaded', () => {
        new ParticipantMeeting();
      });
      
      // Method 1: Add console logging to the existing HostMeeting class
      // Add this line in the constructor after meetingId is set:
      console.log('Meeting ID:', this.meetingId);

      // Method 2: Create a global function to access meeting ID
      // Add this after the HostMeeting class definition:
      window.getMeetingId = function() {
        // Extract from URL (same logic as in the constructor)
        const meetingId = window.location.pathname.split('/').pop();
        console.log('Current Meeting ID:', meetingId);
        return meetingId;
      };

      // Method 3: Store the meeting instance globally for console access
      // Modify the DOMContentLoaded event listener:
      document.addEventListener('DOMContentLoaded', () => {
        window.hostMeeting = new HostMeeting();
        console.log('Host Meeting initialized. Meeting ID:', window.hostMeeting.meetingId);
      });

      // Method 4: Add a dedicated console command function
      window.showMeetingInfo = function() {
        const meetingId = window.location.pathname.split('/').pop();
        const joinUrl = `${window.location.origin}/join/${meetingId}`;
        
        console.group(' Meeting Information');
        console.log('Meeting ID:', meetingId);
        console.log('Join URL:', joinUrl);
        console.log('Current URL:', window.location.href);
        if (window.hostMeeting) {
          console.log('Participants:', window.hostMeeting.participants.size);
          console.log('Is Host:', window.hostMeeting.isHost);
          console.log('User Name:', window.hostMeeting.userName);
        }
        console.groupEnd();
        
        return {
          meetingId,
          joinUrl,
          currentUrl: window.location.href
        };
      };

      // Method 5: Simple one-liner for immediate use
      // You can run this directly in the browser console:
      console.log('Meeting ID:', window.location.pathname.split('/').pop());

      // Method 6: Enhanced version with error handling
      window.getMeetingDetails = function() {
        try {
          const pathParts = window.location.pathname.split('/');
          const meetingId = pathParts[pathParts.length - 1];
          
          if (!meetingId || meetingId === '') {
            console.warn('No meeting ID found in URL');
            return null;
          }
          
          const details = {
            meetingId: meetingId,
            joinUrl: `${window.location.origin}/join/${meetingId}`,
            hostUrl: window.location.href,
            timestamp: new Date().toISOString()
          };
          
          console.table(details);
          return details;
        } catch (error) {
          console.error('Error getting meeting details:', error);
          return null;
        }
      };
      
      // Make host name globally accessible
      var myName = null;
      window.myName = null;

      // Store global reference when meeting initializes  
      window.addEventListener('load', function() {
        setTimeout(() => {
          // Try to find the host name from various sources
          const participantItems = document.querySelectorAll('.participant-item');
          for (let item of participantItems) {
            const roleElement = item.querySelector('.role-badge');
            if (roleElement && roleElement.textContent.includes('Host')) {
              const nameElement = item.querySelector('.participant-name');
              if (nameElement) {
                myName = nameElement.textContent.trim();
                window.myName = myName;
                console.log('myName set to:', myName);
                return;
              }
            }
          }
        }, 3000); // Wait 3 seconds for everything to load
      });
      // Permanently remove own video element in mobile view only
(function() {
  const isMobile = () => /Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
  
  if (!isMobile()) {
    console.log('Desktop view - own video will be visible');
    return; // Exit entirely on desktop
  }
  
  console.log('Mobile view detected - will remove own video');
  
  function removeOwnVideo() {
    // Try to get socket ID from window.hostMeetingInstance
    let localSocketId = null;
    
    if (window.hostMeetingInstance && window.hostMeetingInstance.socket) {
      localSocketId = window.hostMeetingInstance.socket.id;
    } else if (window.socket) {
      localSocketId = window.socket.id;
    }
    
    if (!localSocketId) {
      console.log('Waiting for socket ID...');
      setTimeout(removeOwnVideo, 500);
      return;
    }
    
    console.log('Local socket ID:', localSocketId);
    
    const localVideoWrapper = document.querySelector(`[data-socket-id="${localSocketId}"]`);
    
    if (localVideoWrapper) {
      console.log('Found own video wrapper, removing permanently...');
      localVideoWrapper.remove();
      console.log('Own video element removed from DOM');
    } else {
      console.log('Own video wrapper not found yet, retrying...');
      setTimeout(removeOwnVideo, 500);
    }
  }
  
  // Hook into the renderParticipants method to remove own video after each render
  function interceptRenderParticipants() {
    if (!window.hostMeetingInstance) {
      setTimeout(interceptRenderParticipants, 500);
      return;
    }
    
    const originalRender = window.hostMeetingInstance.renderParticipants;
    
    window.hostMeetingInstance.renderParticipants = function() {
      // Call original render
      originalRender.call(this);
      
      // Then remove own video if it was created
      setTimeout(() => {
        if (this.socket && this.socket.id) {
          const ownVideo = document.querySelector(`[data-socket-id="${this.socket.id}"]`);
          if (ownVideo) {
            console.log('Removing own video after render');
            ownVideo.remove();
          }
        }
      }, 50);
    };
    
    console.log('Intercepted renderParticipants method');
  }
  
  // Start the removal process
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      setTimeout(removeOwnVideo, 1000);
      setTimeout(interceptRenderParticipants, 1500);
    });
  } else {
    setTimeout(removeOwnVideo, 1000);
    setTimeout(interceptRenderParticipants, 1500);
  }
})();