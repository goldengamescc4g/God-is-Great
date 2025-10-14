// Mobile Controls for Video Call
// This script adds mobile-optimized controls with 5 bottom buttons

(function() {
  'use strict';

  // Only initialize on mobile devices
  function isMobileDevice() {
    return window.innerWidth <= 768;
  }

  // Initialize mobile controls
  function initMobileControls() {
    if (!isMobileDevice()) return;

    createMobileControlBar();
    createMoreMenu();
    createMobileMeetingInfo();
    setupEventListeners();
    observeParticipantCount();
  }

  // Create the mobile control bar with 5 buttons
  function createMobileControlBar() {
    const existingBar = document.getElementById('mobile-controls-bar');
    if (existingBar) existingBar.remove();

    const controlBar = document.createElement('div');
    controlBar.className = 'mobile-controls-bar';
    controlBar.id = 'mobile-controls-bar';

    controlBar.innerHTML = `
      <!-- More Menu Button -->
      <button class="mobile-control-btn more-menu" id="mobile-more-btn" aria-label="More options">
        <i class="fas fa-ellipsis-h"></i>
      </button>

      <!-- Camera Toggle -->
      <button class="mobile-control-btn active" id="mobile-camera-btn" aria-label="Toggle camera">
        <i class="fas fa-video"></i>
      </button>

      <!-- Microphone Toggle -->
      <button class="mobile-control-btn active" id="mobile-mic-btn" aria-label="Toggle microphone">
        <i class="fas fa-microphone"></i>
      </button>
      

      <!-- Participants Button -->
      <button class="mobile-control-btn" id="mobile-participants-btn" aria-label="View participants">
        <i class="fas fa-users"></i>
        <span class="mobile-participant-badge" id="mobile-participant-count">0</span>
      </button>
      

      <!-- End Call Button -->
      <button class="mobile-control-btn end-call" id="mobile-end-call-btn" aria-label="End call">
        <i class="fas fa-phone-slash"></i>
      </button>
    `;

    document.body.appendChild(controlBar);
  }

  // Create the more menu panel
  function createMoreMenu() {
    const existingMenu = document.getElementById('more-menu-container');
    if (existingMenu) existingMenu.remove();

    const menuContainer = document.createElement('div');
    menuContainer.id = 'more-menu-container';

    menuContainer.innerHTML = `
      <!-- Overlay -->
      <div class="more-menu-overlay" id="more-menu-overlay"></div>

      <!-- Menu Panel -->
      <div class="more-menu-panel" id="more-menu-panel">
        <div class="menu-handle"></div>
        <h3 class="menu-title">More Options</h3>

        <div class="menu-items-grid">
          <!-- Screen Share -->
          <button class="menu-item" id="menu-screen-share">
            <div class="menu-item-icon">
              <i class="fas fa-desktop"></i>
            </div>
            <div class="menu-item-label">Share Screen</div>
          </button>

          <!-- Chat -->
          <button class="menu-item" id="menu-chat">
            <div class="menu-item-icon">
              <i class="fas fa-comment"></i>
            </div>
            <div class="menu-item-label">Chat</div>
          </button>

          <!-- Raise Hand -->
          <button class="menu-item" id="menu-raise-hand">
            <div class="menu-item-icon">
              <i class="fas fa-hand-paper"></i>
            </div>
            <div class="menu-item-label">Raise Hand</div>
          </button>

          <!-- Share Files -->
          <button class="menu-item" id="menu-share-files">
           <div class="menu-item-icon">
  <i class="fas fa-file"></i> 
</div>
            <div class="menu-item-label">Share Files</div>
          </button>

          <!-- Settings -->
          <button class="menu-item" id="menu-settings">
            <div class="menu-item-icon">
              <i class="fas fa-cog"></i>
            </div>
            <div class="menu-item-label">Settings</div>
          </button>

          <!-- Notes -->
          <button class="menu-item" id="menu-notes">
            <div class="menu-item-icon">
              <i class="fas fa-sticky-note"></i>
            </div>
            <div class="menu-item-label">Notes</div>
          </button>

          <!-- Record -->
          <button class="menu-item" id="menu-record">
         <div class="menu-item-icon" style="color: red;">
  <i class="fas fa-circle"></i>
</div>

            <div class="menu-item-label">Record</div>
          </button>

          <!-- View Toggle -->
       

          <!-- Meeting Lock -->
          <button class="menu-item" id="menu-meeting-lock">
            <div class="menu-item-icon">
              <i class="fas fa-lock"></i>
            </div>
            <div class="menu-item-label">Lock Meeting</div>
          </button>
        </div>
      </div>
    `;

    document.body.appendChild(menuContainer);
  }

  // Create mobile meeting info header
  function createMobileMeetingInfo() {
    const existingInfo = document.getElementById('mobile-meeting-info');
    if (existingInfo) existingInfo.remove();

    const meetingInfo = document.createElement('div');
    meetingInfo.className = 'mobile-meeting-info';
    meetingInfo.id = 'mobile-meeting-info';
    meetingInfo.style.display = "none"

    const meetingTitle = document.getElementById('meetingTitle')?.textContent || 'Meeting';
    const meetingTime = document.getElementById('meetingTime')?.textContent || '';

    meetingInfo.innerHTML = `
      <div>
        <h3>${meetingTitle}</h3>
        <div class="connection-indicator">
          <div class="connection-dot"></div>
          <span>Connected</span>
        </div>
      </div>
      <div class="time">${meetingTime}</div>
    `;

    document.body.appendChild(meetingInfo);

    // Update time periodically
    setInterval(() => {
      const timeElement = document.querySelector('.mobile-meeting-info .time');
      const now = new Date();
      const timeString = now.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: true
      });
      if (timeElement) timeElement.textContent = timeString;
    }, 60000);
  }

  // Setup all event listeners
  function setupEventListeners() {
    // More menu toggle
    const moreBtn = document.getElementById('mobile-more-btn');
    const overlay = document.getElementById('more-menu-overlay');
    const panel = document.getElementById('more-menu-panel');

    if (moreBtn && overlay && panel) {
      moreBtn.addEventListener('click', () => toggleMoreMenu());
      overlay.addEventListener('click', () => closeMoreMenu());
    }

    // Camera toggle
    const cameraBtn = document.getElementById('mobile-camera-btn');
    const desktopCameraBtn = document.getElementById('cameraBtn');

    if (cameraBtn) {
      cameraBtn.addEventListener('click', () => {
        if (desktopCameraBtn) {
          desktopCameraBtn.click();
        }
        toggleButtonState(cameraBtn);
      });

      // Sync with desktop button
      if (desktopCameraBtn) {
        syncButtonState(desktopCameraBtn, cameraBtn, 'fa-video', 'fa-video-slash');
      }
    }

    // Microphone toggle
    const micBtn = document.getElementById('mobile-mic-btn');
    const desktopMicBtn = document.getElementById('micBtn');

    if (micBtn) {
      micBtn.addEventListener('click', () => {
        if (desktopMicBtn) {
          desktopMicBtn.click();
        }
        toggleButtonState(micBtn);
      });

      // Sync with desktop button
      if (desktopMicBtn) {
        syncButtonState(desktopMicBtn, micBtn, 'fa-microphone', 'fa-microphone-slash');
      }
    }

    // Participants toggle
    const participantsBtn = document.getElementById('mobile-participants-btn');
    const desktopParticipantsBtn = document.getElementById('memberToggleBtn');

    if (participantsBtn && desktopParticipantsBtn) {
      participantsBtn.addEventListener('click', () => {
        desktopParticipantsBtn.click();
        closeMoreMenu();
      });
    }

    // End call
    const endCallBtn = document.getElementById('mobile-end-call-btn');
    const desktopEndCallBtn = document.getElementById('leaveCallBtn');

    if (endCallBtn && desktopEndCallBtn) {
      endCallBtn.addEventListener('click', () => {
        desktopEndCallBtn.click();
      });
    }

    // Menu items
    setupMenuItem('menu-screen-share', 'screenShareBtn');
    setupMenuItem('menu-chat', 'chat-btn', toggleChat);
    setupMenuItem('menu-raise-hand', null, raiseHand);
    setupMenuItem('menu-share-files', 'floating-share-btn', () => handleToolClick('share'));
    setupMenuItem('menu-settings', 'settings-btn');
    setupMenuItem('menu-notes', 'notesBtn', () => handleToolClick('notes'));
    setupMenuItem('menu-record', 'startCaptureBtn');
    setupMenuItem('menu-view-toggle', 'viewToggle', updateViewToggleLabel);
    setupMenuItem('menu-meeting-lock', 'vortex-primary-activator-3k7s');
  }

  // Setup individual menu item
  function setupMenuItem(menuItemId, desktopBtnId, callback) {
    const menuItem = document.getElementById(menuItemId);

    if (menuItem) {
      menuItem.addEventListener('click', () => {
        if (desktopBtnId) {
          const desktopBtn = document.getElementById(desktopBtnId);
          if (desktopBtn) {
            desktopBtn.click();
          }
        }

        if (callback) {
          callback();
        }

        closeMoreMenu();
      });
    }
  }

  // Toggle more menu
  function toggleMoreMenu() {
    const overlay = document.getElementById('more-menu-overlay');
    const panel = document.getElementById('more-menu-panel');
    const moreBtn = document.getElementById('mobile-more-btn');

    if (overlay && panel && moreBtn) {
      const isActive = overlay.classList.contains('active');

      if (isActive) {
        closeMoreMenu();
      } else {
        overlay.classList.add('active');
        panel.classList.add('active');
        moreBtn.classList.add('active');
      }
    }
  }

  // Close more menu
  function closeMoreMenu() {
    const overlay = document.getElementById('more-menu-overlay');
    const panel = document.getElementById('more-menu-panel');
    const moreBtn = document.getElementById('mobile-more-btn');

    if (overlay && panel && moreBtn) {
      overlay.classList.remove('active');
      panel.classList.remove('active');
      moreBtn.classList.remove('active');
    }
  }

  // Toggle button state (active/inactive)
  function toggleButtonState(button) {
    if (button.classList.contains('active')) {
      button.classList.remove('active');
      button.classList.add('inactive');
    } else {
      button.classList.remove('inactive');
      button.classList.add('active');
    }
  }

  // Sync mobile button with desktop button state
  function syncButtonState(desktopBtn, mobileBtn, iconOn, iconOff) {
    const observer = new MutationObserver(() => {
      const isActive = desktopBtn.getAttribute('data-active') === 'true';
      const icon = mobileBtn.querySelector('i');

      if (isActive) {
        mobileBtn.classList.remove('inactive');
        mobileBtn.classList.add('active');
        if (icon) icon.className = `fas ${iconOn}`;
      } else {
        mobileBtn.classList.remove('active');
        mobileBtn.classList.add('inactive');
        if (icon) icon.className = `fas ${iconOff}`;
      }
    });

    observer.observe(desktopBtn, {
      attributes: true,
      attributeFilter: ['data-active']
    });

    // Initial sync
    const isActive = desktopBtn.getAttribute('data-active') === 'true';
    const icon = mobileBtn.querySelector('i');

    if (isActive) {
      mobileBtn.classList.add('active');
      if (icon) icon.className = `fas ${iconOn}`;
    } else {
      mobileBtn.classList.add('inactive');
      if (icon) icon.className = `fas ${iconOff}`;
    }
  }

  // Observe participant count changes
  function observeParticipantCount() {
    const desktopCount = document.getElementById('participantCount');
    const mobileCount = document.getElementById('mobile-participant-count');

    if (desktopCount && mobileCount) {
      const observer = new MutationObserver(() => {
        mobileCount.textContent = desktopCount.textContent;
      });

      observer.observe(desktopCount, {
        childList: true,
        characterData: true,
        subtree: true
      });

      // Initial sync
      mobileCount.textContent = desktopCount.textContent;
    }
  }

  // Raise hand function
  function raiseHand() {
    if (typeof window.toggleHandRaise === 'function') {
      window.toggleHandRaise();
    }
  }

  // Update view toggle label
  function updateViewToggleLabel() {
    setTimeout(() => {
      const viewToggleText = document.getElementById('viewToggleText');
      const menuViewToggle = document.getElementById('menu-view-toggle');

      if (viewToggleText && menuViewToggle) {
        const label = menuViewToggle.querySelector('.menu-item-label');
        if (label) {
          label.textContent = viewToggleText.textContent;
        }
      }
    }, 100);
  }

  // Handle window resize
  let resizeTimeout;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      if (isMobileDevice() && !document.getElementById('mobile-controls-bar')) {
        initMobileControls();
      } else if (!isMobileDevice()) {
        const mobileBar = document.getElementById('mobile-controls-bar');
        const menuContainer = document.getElementById('more-menu-container');
        const meetingInfo = document.getElementById('mobile-meeting-info');

        if (mobileBar) mobileBar.remove();
        if (menuContainer) menuContainer.remove();
        if (meetingInfo) meetingInfo.remove();
      }
    }, 250);
  });

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMobileControls);
  } else {
    // DOM is already loaded
    setTimeout(initMobileControls, 500);
  }
})();
// Mobile Meeting Enhancements - WhatsApp-style Video Call

class MobileMeetingEnhancer {
  constructor() {
    this.isMobile = this.detectMobile();
    this.selfViewPosition = 'top-right';
    this.isDragging = false;
    this.dragStartX = 0;
    this.dragStartY = 0;
    this.dragStartLeft = 0;
    this.dragStartTop = 0;
    this.selfViewContainer = null;
    this.toolsPanel = null;

    if (this.isMobile) {
      this.init();
    }
  }

  detectMobile() {
    return window.innerWidth <= 768 ||
           /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
  }

  init() {
    console.log('Initializing mobile meeting enhancements...');

    // Wait for DOM to be ready
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => this.setup());
    } else {
      this.setup();
    }

    // Handle orientation changes
    window.addEventListener('orientationchange', () => {
      setTimeout(() => this.adjustLayout(), 300);
    });

    // Handle resize
    let resizeTimer;
    window.addEventListener('resize', () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => this.adjustLayout(), 250);
    });
  }

  setup() {
    this.createMobileSelfView();
    this.enhanceToolsPanel();
    this.setupMainVideoView();
    this.addConnectionIndicator();
    this.observeParticipantChanges();

    // Check periodically if elements exist
    this.checkInterval = setInterval(() => {
      if (!this.selfViewContainer || !document.body.contains(this.selfViewContainer)) {
        this.createMobileSelfView();
      } else {
        // Re-apply mobile classes when participants change
        this.applyMobileParticipantClasses();
      }
    }, 2000);
  }

  observeParticipantChanges() {
    const secondarySection = document.getElementById('secondaryVideosSection');
    if (!secondarySection) {
      setTimeout(() => this.observeParticipantChanges(), 1000);
      return;
    }

    // Observe changes to participant videos
    const observer = new MutationObserver(() => {
      this.applyMobileParticipantClasses();
    });

    observer.observe(secondarySection, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['style']
    });

    console.log('Mobile participant observer initialized');
  }

  createMobileSelfView() {
    // Find local video (the user's own video)
    const videoWrappers = document.querySelectorAll('.video-wrapper');
    let localVideoWrapper = null;

    videoWrappers.forEach(wrapper => {
      const socketId = wrapper.dataset.socketId;
      if (window.socket && socketId === window.socket.id) {
        localVideoWrapper = wrapper;
      }
    });

    if (!localVideoWrapper) {
      // Try again later
      return;
    }

    // Create self-view container if it doesn't exist
    if (!this.selfViewContainer || !document.body.contains(this.selfViewContainer)) {
      this.selfViewContainer = document.createElement('div');
      const savedPosition = localStorage.getItem('selfViewPosition') || this.selfViewPosition;
      this.selfViewPosition = savedPosition;
      this.selfViewContainer.className = `self-view-container position-${this.selfViewPosition}`;
      document.body.appendChild(this.selfViewContainer);
    }

    // Clone the video element
    const videoFrame = localVideoWrapper.querySelector('.video-frame');
    const participantName = localVideoWrapper.querySelector('.participant-name');

    if (videoFrame) {
      // Create a new video element that shares the same stream
      const selfVideo = document.createElement('video');
      selfVideo.className = 'video-frame';
      selfVideo.autoplay = true;
      selfVideo.muted = true;
      selfVideo.playsinline = true;
      selfVideo.srcObject = videoFrame.srcObject;

      // Clear and populate self-view container
      this.selfViewContainer.innerHTML = '';
      this.selfViewContainer.appendChild(selfVideo);

      if (participantName) {
        const nameClone = participantName.cloneNode(true);
        this.selfViewContainer.appendChild(nameClone);
      }

      // Keep video stream in sync
      const updateStream = () => {
        if (videoFrame.srcObject && selfVideo.srcObject !== videoFrame.srcObject) {
          selfVideo.srcObject = videoFrame.srcObject;
        }
      };

      // Monitor for stream changes
      this.streamMonitor = setInterval(updateStream, 1000);

      // Hide the original video wrapper in sidebar (exclude from grid)
      if (localVideoWrapper.parentElement &&
          localVideoWrapper.parentElement.classList.contains('secondary-videos-section')) {
        localVideoWrapper.style.display = 'none';
      }
    }

    // Setup drag functionality
    this.setupDragFunctionality();

    // Apply mobile participant grid classes
    this.applyMobileParticipantClasses();
  }

  setupDragFunctionality() {
    if (!this.selfViewContainer) return;

    // Touch events
    this.selfViewContainer.addEventListener('touchstart', (e) => this.handleDragStart(e), { passive: false });
    this.selfViewContainer.addEventListener('touchmove', (e) => this.handleDragMove(e), { passive: false });
    this.selfViewContainer.addEventListener('touchend', (e) => this.handleDragEnd(e), { passive: false });

    // Mouse events (for testing on desktop)
    this.selfViewContainer.addEventListener('mousedown', (e) => this.handleDragStart(e));
    document.addEventListener('mousemove', (e) => this.handleDragMove(e));
    document.addEventListener('mouseup', (e) => this.handleDragEnd(e));
  }

  handleDragStart(e) {
    if (!this.selfViewContainer) return;

    this.isDragging = true;
    this.selfViewContainer.classList.add('dragging');

    const touch = e.touches ? e.touches[0] : e;
    this.dragStartX = touch.clientX;
    this.dragStartY = touch.clientY;

    const rect = this.selfViewContainer.getBoundingClientRect();
    this.dragStartLeft = rect.left;
    this.dragStartTop = rect.top;

    // Remove position classes to enable free movement
    this.selfViewContainer.classList.remove(
      'position-top-right',
      'position-top-left',
      'position-bottom-right',
      'position-bottom-left'
    );

    // Set initial position for smooth dragging
    this.selfViewContainer.style.left = `${rect.left}px`;
    this.selfViewContainer.style.top = `${rect.top}px`;
    this.selfViewContainer.style.right = 'auto';
    this.selfViewContainer.style.bottom = 'auto';

    e.preventDefault();
    e.stopPropagation();
  }

  handleDragMove(e) {
    if (!this.isDragging || !this.selfViewContainer) return;

    const touch = e.touches ? e.touches[0] : e;
    const deltaX = touch.clientX - this.dragStartX;
    const deltaY = touch.clientY - this.dragStartY;

    let newLeft = this.dragStartLeft + deltaX;
    let newTop = this.dragStartTop + deltaY;

    // Constrain to viewport with better margins
    const rect = this.selfViewContainer.getBoundingClientRect();
    const maxLeft = window.innerWidth - rect.width - 8;
    const maxTop = window.innerHeight - rect.height - 100;

    newLeft = Math.max(8, Math.min(newLeft, maxLeft));
    newTop = Math.max(60, Math.min(newTop, maxTop));

    this.selfViewContainer.style.left = `${newLeft}px`;
    this.selfViewContainer.style.top = `${newTop}px`;

    e.preventDefault();
    e.stopPropagation();
  }

  handleDragEnd(e) {
    if (!this.isDragging || !this.selfViewContainer) return;

    this.isDragging = false;
    this.selfViewContainer.classList.remove('dragging');

    // Snap to nearest corner with smooth animation
    this.snapToCorner();

    e.preventDefault();
    e.stopPropagation();
  }

  snapToCorner() {
    if (!this.selfViewContainer) return;

    const rect = this.selfViewContainer.getBoundingClientRect();
    const centerX = rect.left + rect.width / 2;
    const centerY = rect.top + rect.height / 2;

    const isLeft = centerX < window.innerWidth / 2;
    const isTop = centerY < window.innerHeight / 2;

    // Determine corner
    let position;
    if (isTop && isLeft) {
      position = 'top-left';
    } else if (isTop && !isLeft) {
      position = 'top-right';
    } else if (!isTop && isLeft) {
      position = 'bottom-left';
    } else {
      position = 'bottom-right';
    }

    // Apply position
    this.selfViewPosition = position;

    // Reset inline styles to use CSS positioning
    this.selfViewContainer.style.left = '';
    this.selfViewContainer.style.top = '';
    this.selfViewContainer.style.right = '';
    this.selfViewContainer.style.bottom = '';

    this.selfViewContainer.classList.remove(
      'position-top-right',
      'position-top-left',
      'position-bottom-right',
      'position-bottom-left'
    );
    this.selfViewContainer.classList.add(`position-${position}`);

    // Save preference
    localStorage.setItem('selfViewPosition', position);
  }

  applyMobileParticipantClasses() {
    if (!this.isMobile) return;

    const secondarySection = document.getElementById('secondaryVideosSection');
    if (!secondarySection) return;

    // Count visible participants (excluding self-view)
    const visibleWrappers = Array.from(document.querySelectorAll('.secondary-videos-section .video-wrapper'))
      .filter(wrapper => wrapper.style.display !== 'none');

    const participantCount = visibleWrappers.length;

    // Remove all mobile participant classes
    secondarySection.classList.remove(
      'mobile-participants-2',
      'mobile-participants-3',
      'mobile-participants-4',
      'mobile-participants-5',
      'mobile-participants-6',
      'mobile-participants-7',
      'mobile-participants-8',
      'mobile-participants-9'
    );

    // Add appropriate class based on count
    if (participantCount >= 2 && participantCount <= 9) {
      secondarySection.classList.add(`mobile-participants-${participantCount}`);
    }

    console.log(`Applied mobile layout for ${participantCount} participants`);
  }

  enhanceToolsPanel() {
    const toolsPanel = document.getElementById('dimension-tools-overlay-4h9w');
    if (!toolsPanel) return;

    this.toolsPanel = toolsPanel;

    // Add handle for better UX
    const header = toolsPanel.querySelector('.eclipse-header-9m2x');
    if (header && !header.querySelector('.popup-handle')) {
      const handle = document.createElement('div');
      handle.className = 'popup-handle';
      header.insertBefore(handle, header.firstChild);
    }

    // Close on swipe down
    let startY = 0;
    let currentY = 0;

    header?.addEventListener('touchstart', (e) => {
      startY = e.touches[0].clientY;
    }, { passive: true });

    header?.addEventListener('touchmove', (e) => {
      currentY = e.touches[0].clientY;
      const deltaY = currentY - startY;

      if (deltaY > 0 && toolsPanel.classList.contains('active')) {
        toolsPanel.style.transform = `translateY(${deltaY}px)`;
      }
    }, { passive: true });

    header?.addEventListener('touchend', () => {
      const deltaY = currentY - startY;

      if (deltaY > 100) {
        // Close panel
        toolsPanel.classList.remove('active');
      }

      toolsPanel.style.transform = '';
      startY = 0;
      currentY = 0;
    }, { passive: true });

    // Observe when panel opens/closes
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (mutation.attributeName === 'class') {
          const isActive = toolsPanel.classList.contains('active');
          if (isActive) {
            this.onToolsPanelOpen();
          }
        }
      });
    });

    observer.observe(toolsPanel, { attributes: true });
  }

  onToolsPanelOpen() {
    // Ensure proper scrolling
    const grid = document.querySelector('.prism-tool-grid-4j8m');
    if (grid) {
      grid.scrollTop = 0;
    }
  }

  setupMainVideoView() {
    // Ensure main video section shows the active speaker or first participant
    const updateMainVideo = () => {
      const mainSection = document.getElementById('mainVideoSection');
      const secondarySection = document.getElementById('secondaryVideosSection');

      if (!mainSection || !secondarySection) return;

      const videoWrappers = document.querySelectorAll('.video-wrapper');
      if (videoWrappers.length === 0) return;

      // Find remote participant (not self)
      let remoteWrapper = null;
      videoWrappers.forEach(wrapper => {
        const socketId = wrapper.dataset.socketId;
        if (window.socket && socketId !== window.socket.id) {
          if (!remoteWrapper) {
            remoteWrapper = wrapper;
          }
        }
      });

      if (remoteWrapper && !mainSection.contains(remoteWrapper)) {
        // Move to main section
        mainSection.innerHTML = '';
        const clone = remoteWrapper.cloneNode(true);

        // Copy video stream
        const originalVideo = remoteWrapper.querySelector('.video-frame');
        const clonedVideo = clone.querySelector('.video-frame');
        if (originalVideo && clonedVideo) {
          clonedVideo.srcObject = originalVideo.srcObject;
        }

        mainSection.appendChild(clone);

      
      }
    };

    // Update periodically
    setInterval(updateMainVideo, 2000);
    updateMainVideo();
  }

  addConnectionIndicator() {
    // Create connection quality indicator
    const indicator = document.createElement('div');
    indicator.className = 'connection-indicator';
    indicator.innerHTML = '<i class="fas fa-wifi"></i> <span>Good Connection</span>';
    document.body.appendChild(indicator);

    this.connectionIndicator = indicator;

    // Monitor connection (simplified)
    if (window.socket) {
      window.socket.on('ping', () => {
        this.showConnectionStatus('good');
      });

      window.socket.on('disconnect', () => {
        this.showConnectionStatus('poor');
      });
    }
  }

  showConnectionStatus(status) {
    if (!this.connectionIndicator) return;

    this.connectionIndicator.classList.remove('good', 'poor');
    this.connectionIndicator.classList.add(status);
    this.connectionIndicator.classList.add('show');

    const text = this.connectionIndicator.querySelector('span');
    if (text) {
      text.textContent = status === 'good' ? 'Good Connection' : 'Poor Connection';
    }

    // Auto-hide after 3 seconds
    setTimeout(() => {
      this.connectionIndicator.classList.remove('show');
    }, 3000);
  }

  adjustLayout() {
    // Re-adjust layout on orientation change
    if (this.selfViewContainer) {
      this.snapToCorner();
    }
  }

  destroy() {
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
    }
    if (this.streamMonitor) {
      clearInterval(this.streamMonitor);
    }
    if (this.selfViewContainer) {
      this.selfViewContainer.remove();
    }
    if (this.connectionIndicator) {
      this.connectionIndicator.remove();
    }
  }
}

// Auto-initialize when ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    window.mobileMeetingEnhancer = new MobileMeetingEnhancer();
  });
} else {
  window.mobileMeetingEnhancer = new MobileMeetingEnhancer();
}

// Export for manual control
window.MobileMeetingEnhancer = MobileMeetingEnhancer;
