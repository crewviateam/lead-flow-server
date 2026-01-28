// services/WorkingDaysService.js
// Single Source of Truth for working days validation
// Handles paused dates, weekend days, and working day calculations

const moment = require('moment-timezone');

class WorkingDaysService {
  
  /**
   * Check if a date is a working day (not weekend, not paused)
   * @param {Date|string} date - The date to check
   * @param {Object} settings - Settings object containing weekendDays and pausedDates
   * @returns {Object} { isWorkingDay: boolean, reason?: string }
   */
  isWorkingDay(date, settings) {
    const dateMoment = moment(date);
    const dayOfWeek = dateMoment.day(); // 0 = Sunday, 6 = Saturday
    const dateStr = dateMoment.format('YYYY-MM-DD');
    
    // Check weekend days
    const weekendDays = settings?.businessHours?.weekendDays || settings?.weekendDays || [0, 6];
    if (weekendDays.includes(dayOfWeek)) {
      return { 
        isWorkingDay: false, 
        reason: `${dateMoment.format('dddd')} is a weekend day` 
      };
    }
    
    // Check paused dates
    const pausedDates = settings?.pausedDates || [];
    const isPaused = pausedDates.some(pd => 
      moment(pd).format('YYYY-MM-DD') === dateStr
    );
    
    if (isPaused) {
      return { 
        isWorkingDay: false, 
        reason: `${dateStr} is a paused date` 
      };
    }
    
    return { isWorkingDay: true };
  }
  
  /**
   * Get the next working day from a given date
   * @param {Date|string} fromDate - Starting date
   * @param {Object} settings - Settings object
   * @param {number} startHour - Hour to set for the returned date (default 8)
   * @returns {Date}
   */
  getNextWorkingDay(fromDate, settings, startHour = 8) {
    let currentDate = moment(fromDate);
    let attempts = 0;
    const maxAttempts = 365; // Prevent infinite loop
    
    while (attempts < maxAttempts) {
      const check = this.isWorkingDay(currentDate, settings);
      
      if (check.isWorkingDay) {
        return currentDate.hour(startHour).minute(0).second(0).toDate();
      }
      
      currentDate = currentDate.add(1, 'day').startOf('day');
      attempts++;
    }
    
    // Fallback: return original date if all days are blocked
    console.warn('[WorkingDaysService] All days appear blocked, returning original date');
    return moment(fromDate).hour(startHour).minute(0).second(0).toDate();
  }
  
  /**
   * Adjust a date to the next working day if current day is not working
   * @param {Date|string} date 
   * @param {Object} settings 
   * @param {number} startHour 
   * @returns {Date}
   */
  adjustToWorkingDay(date, settings, startHour = 8) {
    const check = this.isWorkingDay(date, settings);
    
    if (check.isWorkingDay) {
      return new Date(date);
    }
    
    return this.getNextWorkingDay(date, settings, startHour);
  }
  
  /**
   * Check if a date is a paused date
   * @param {Date|string} date 
   * @param {Object} settings 
   * @returns {boolean}
   */
  isPausedDate(date, settings) {
    const dateStr = moment(date).format('YYYY-MM-DD');
    const pausedDates = settings?.pausedDates || [];
    
    return pausedDates.some(pd => moment(pd).format('YYYY-MM-DD') === dateStr);
  }
  
  /**
   * Get list of paused dates in a range (for UI display)
   * @param {Date|string} startDate 
   * @param {Date|string} endDate 
   * @param {Object} settings 
   * @returns {string[]} Array of YYYY-MM-DD strings
   */
  getPausedDatesInRange(startDate, endDate, settings) {
    const pausedDates = settings?.pausedDates || [];
    const start = moment(startDate);
    const end = moment(endDate);
    
    return pausedDates
      .map(pd => moment(pd).format('YYYY-MM-DD'))
      .filter(pdStr => {
        const pdMoment = moment(pdStr);
        return pdMoment.isSameOrAfter(start, 'day') && pdMoment.isSameOrBefore(end, 'day');
      });
  }
  
  /**
   * Get weekend days configuration
   * @param {Object} settings 
   * @returns {number[]} Array of day indices (0=Sunday, 6=Saturday)
   */
  getWeekendDays(settings) {
    return settings?.businessHours?.weekendDays || settings?.weekendDays || [0, 6];
  }
}

module.exports = new WorkingDaysService();
