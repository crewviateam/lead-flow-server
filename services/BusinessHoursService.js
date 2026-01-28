// services/BusinessHoursService.js
// Single Source of Truth for business hours validation
// Provides timezone-aware business hours checking and adjustment

const moment = require('moment-timezone');

class BusinessHoursService {
  
  /**
   * Check if a given time is within business hours for a specific timezone
   * @param {Date|string} targetTime - The time to check
   * @param {string} timezone - IANA timezone (e.g., 'America/New_York')
   * @param {Object} settings - Settings object containing businessHours
   * @returns {Object} { valid: boolean, localHour: number, message: string }
   */
  isWithinBusinessHours(targetTime, timezone, settings) {
    const businessHours = settings?.businessHours || { startHour: 8, endHour: 22 };
    const startHour = businessHours.startHour ?? 8;
    const endHour = businessHours.endHour ?? 22;
    
    const localMoment = moment(targetTime).tz(timezone);
    const localHour = localMoment.hour();
    const localMinute = localMoment.minute();
    
    // Check if within business hours
    const isValid = localHour >= startHour && localHour < endHour;
    
    return {
      valid: isValid,
      localHour,
      localMinute,
      localTime: localMoment.format('HH:mm'),
      timezone,
      businessStart: startHour,
      businessEnd: endHour,
      message: isValid 
        ? `Time ${localMoment.format('HH:mm')} is within business hours (${startHour}:00-${endHour}:00)`
        : `Time ${localMoment.format('HH:mm')} is outside business hours (${startHour}:00-${endHour}:00)`
    };
  }
  
  /**
   * Validate a time for rescheduling - returns error details if invalid
   * @param {Date|string} targetTime 
   * @param {string} timezone 
   * @param {Object} settings 
   * @returns {Object} { valid: boolean, error?: string, suggestedTime?: Date }
   */
  validateForReschedule(targetTime, timezone, settings) {
    const check = this.isWithinBusinessHours(targetTime, timezone, settings);
    
    if (check.valid) {
      return { valid: true };
    }
    
    // Calculate suggested time
    const suggestedTime = this.getNextValidSlot(targetTime, timezone, settings);
    
    return {
      valid: false,
      error: `Cannot schedule at ${check.localTime} ${timezone} - outside business hours (${check.businessStart}:00-${check.businessEnd}:00)`,
      suggestedTime,
      localTime: check.localTime,
      businessHours: { start: check.businessStart, end: check.businessEnd }
    };
  }
  
  /**
   * Get the next valid business hour slot from a given time
   * @param {Date|string} fromTime 
   * @param {string} timezone 
   * @param {Object} settings 
   * @param {number} roundToMinutes - Round to nearest X minutes (default 15)
   * @returns {Date}
   */
  getNextValidSlot(fromTime, timezone, settings, roundToMinutes = 15) {
    const businessHours = settings?.businessHours || { startHour: 8, endHour: 22 };
    const startHour = businessHours.startHour ?? 8;
    const endHour = businessHours.endHour ?? 22;
    
    let localMoment = moment(fromTime).tz(timezone);
    const currentHour = localMoment.hour();
    
    // If before business hours, move to start hour
    if (currentHour < startHour) {
      localMoment.hour(startHour).minute(0).second(0);
    }
    // If after business hours, move to next day start hour
    else if (currentHour >= endHour) {
      localMoment.add(1, 'day').hour(startHour).minute(0).second(0);
    }
    // If within business hours, just round to nearest slot
    else {
      const minutes = localMoment.minute();
      const roundedMinutes = Math.ceil(minutes / roundToMinutes) * roundToMinutes;
      
      if (roundedMinutes >= 60) {
        localMoment.add(1, 'hour').minute(0);
        // Check if this pushed us past end hour
        if (localMoment.hour() >= endHour) {
          localMoment.add(1, 'day').hour(startHour).minute(0);
        }
      } else {
        localMoment.minute(roundedMinutes);
      }
      localMoment.second(0);
    }
    
    return localMoment.toDate();
  }
  
  /**
   * Adjust a time to be within business hours
   * If already valid, returns the original time (possibly rounded)
   * @param {Date|string} targetTime 
   * @param {string} timezone 
   * @param {Object} settings 
   * @returns {Date}
   */
  adjustToBusinessHours(targetTime, timezone, settings) {
    const check = this.isWithinBusinessHours(targetTime, timezone, settings);
    
    if (check.valid) {
      // Already valid, just return (caller can round if needed)
      return new Date(targetTime);
    }
    
    return this.getNextValidSlot(targetTime, timezone, settings);
  }
  
  /**
   * Get business hours for display purposes
   * @param {Object} settings 
   * @returns {Object} { startHour, endHour, displayString }
   */
  getBusinessHoursDisplay(settings) {
    const businessHours = settings?.businessHours || { startHour: 8, endHour: 22 };
    const startHour = businessHours.startHour ?? 8;
    const endHour = businessHours.endHour ?? 22;
    
    const formatHour = (h) => {
      const suffix = h >= 12 ? 'PM' : 'AM';
      const hour12 = h > 12 ? h - 12 : (h === 0 ? 12 : h);
      return `${hour12}:00 ${suffix}`;
    };
    
    return {
      startHour,
      endHour,
      displayString: `${formatHour(startHour)} - ${formatHour(endHour)}`,
      display24h: `${startHour.toString().padStart(2, '0')}:00 - ${endHour.toString().padStart(2, '0')}:00`
    };
  }
  
  /**
   * Check if a slot hour is within business hours (for UI filtering)
   * @param {number} hour - Hour in 24h format (0-23)
   * @param {Object} settings 
   * @returns {boolean}
   */
  isHourWithinBusinessHours(hour, settings) {
    const businessHours = settings?.businessHours || { startHour: 8, endHour: 22 };
    return hour >= (businessHours.startHour ?? 8) && hour < (businessHours.endHour ?? 22);
  }
}

module.exports = new BusinessHoursService();
