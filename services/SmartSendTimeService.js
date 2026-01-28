// services/SmartSendTimeService.js
// Single Source of Truth for smart send time optimization
// Determines optimal email sending windows based on engagement patterns

const moment = require('moment-timezone');

class SmartSendTimeService {
  
  /**
   * Check if smart send time is enabled
   * @param {Object} settings 
   * @returns {boolean}
   */
  isEnabled(settings) {
    return settings?.smartSendTime?.enabled === true;
  }
  
  /**
   * Get smart send time windows
   * @param {Object} settings 
   * @returns {Object} { morning: {start, end}, afternoon: {start, end}, priority }
   */
  getWindows(settings) {
    const smartSettings = settings?.smartSendTime || {};
    
    return {
      morning: {
        startHour: smartSettings.morningWindow?.startHour || 9,
        endHour: smartSettings.morningWindow?.endHour || 11
      },
      afternoon: {
        startHour: smartSettings.afternoonWindow?.startHour || 14,
        endHour: smartSettings.afternoonWindow?.endHour || 16
      },
      priority: smartSettings.priority || 'morning'
    };
  }
  
  /**
   * Check if a time falls within a smart send window
   * @param {Date|string} time 
   * @param {string} timezone 
   * @param {Object} settings 
   * @returns {Object} { inWindow: boolean, windowName?: string }
   */
  isInSmartWindow(time, timezone, settings) {
    if (!this.isEnabled(settings)) {
      return { inWindow: true, windowName: 'disabled' };
    }
    
    const windows = this.getWindows(settings);
    const localHour = moment(time).tz(timezone).hour();
    
    if (localHour >= windows.morning.startHour && localHour < windows.morning.endHour) {
      return { inWindow: true, windowName: 'morning' };
    }
    
    if (localHour >= windows.afternoon.startHour && localHour < windows.afternoon.endHour) {
      return { inWindow: true, windowName: 'afternoon' };
    }
    
    return { inWindow: false };
  }
  
  /**
   * Optimize a time to fall within smart send windows (if enabled)
   * Does NOT push to next day if within business hours
   * @param {Date|string} baseTime 
   * @param {string} timezone 
   * @param {Object} settings 
   * @returns {Date}
   */
  optimizeTime(baseTime, timezone, settings) {
    // If smart send is disabled, return as-is
    if (!this.isEnabled(settings)) {
      return new Date(baseTime);
    }
    
    const windows = this.getWindows(settings);
    const businessHours = settings?.businessHours || { startHour: 8, endHour: 22 };
    const localMoment = moment(baseTime).tz(timezone);
    const currentHour = localMoment.hour();
    
    // Already in a smart window, use as-is
    const windowCheck = this.isInSmartWindow(baseTime, timezone, settings);
    if (windowCheck.inWindow) {
      return new Date(baseTime);
    }
    
    // Determine target based on priority and current time
    let targetHour = null;
    
    if (windows.priority === 'morning') {
      if (currentHour < windows.morning.startHour) {
        targetHour = windows.morning.startHour;
      } else if (currentHour >= windows.morning.endHour && currentHour < windows.afternoon.startHour) {
        targetHour = windows.afternoon.startHour;
      }
    } else if (windows.priority === 'afternoon') {
      if (currentHour < windows.afternoon.startHour) {
        targetHour = windows.afternoon.startHour;
      }
    } else {
      // Default: nearest window
      if (currentHour < windows.morning.startHour) {
        targetHour = windows.morning.startHour;
      } else if (currentHour >= windows.morning.endHour && currentHour < windows.afternoon.startHour) {
        targetHour = windows.afternoon.startHour;
      }
    }
    
    // If we found a target hour on the same day, use it
    if (targetHour !== null) {
      return localMoment.hour(targetHour).minute(0).second(0).toDate();
    }
    
    // FALLBACK: If past all windows but still within business hours, use current time
    // (don't push to next day just because smart window is missed)
    if (currentHour < businessHours.endHour) {
      return new Date(baseTime);
    }
    
    // Past business hours, push to next day priority window
    const nextDayHour = windows.priority === 'afternoon' 
      ? windows.afternoon.startHour 
      : windows.morning.startHour;
    
    return localMoment.add(1, 'day').hour(nextDayHour).minute(0).second(0).toDate();
  }
}

module.exports = new SmartSendTimeService();
