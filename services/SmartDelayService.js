// services/SmartDelayService.js
// Smart delay calculation respecting working hours, working days, and slot availability
// User-configurable through Settings

const moment = require('moment-timezone');
const { prisma } = require('../lib/prisma');
const { SettingsRepository } = require('../repositories');
const RulebookService = require('./RulebookService');

class SmartDelayService {
  
  /**
   * Calculate the next valid schedule time respecting:
   * 1. User-set delay hours
   * 2. Working hours (configured in Settings)
   * 3. Working days (excludes weekends and paused dates)
   * 4. Slot availability (rate limit check)
   * 
   * @param {Date} baseTime - The starting time for delay calculation
   * @param {number} delayHours - User-configured delay in hours
   * @param {string} leadTimezone - Lead's timezone for proper calculation
   * @returns {Promise<{time: Date, wasShifted: boolean, shiftReason?: string}>}
   */
  async calculateNextValidTime(baseTime, delayHours, leadTimezone = 'UTC') {
    try {
      const settings = await SettingsRepository.getSettings();
      
      // Get configuration from settings
      const businessHoursStart = settings.businessHoursStart || 9;
      const businessHoursEnd = settings.businessHoursEnd || 18;
      const weekendDays = settings.weekendDays || [0, 6]; // Sunday = 0, Saturday = 6
      const pausedDates = (settings.pausedDates || []).map(d => moment(d).format('YYYY-MM-DD'));
      
      // Calculate target time with delay
      let targetTime = moment(baseTime).add(delayHours, 'hours');
      let wasShifted = false;
      let shiftReason = null;
      
      // Convert to lead's timezone for proper hour checks
      const targetInZone = targetTime.clone().tz(leadTimezone || 'UTC');
      
      // STEP 1: Check if target time is within working hours
      const targetHour = targetInZone.hour();
      if (targetHour < businessHoursStart || targetHour >= businessHoursEnd) {
        // Shift to next working hour
        if (targetHour < businessHoursStart) {
          // Before work hours - move to start of day
          targetInZone.hour(businessHoursStart).minute(0).second(0);
        } else {
          // After work hours - move to start of next day
          targetInZone.add(1, 'day').hour(businessHoursStart).minute(0).second(0);
        }
        wasShifted = true;
        shiftReason = 'Shifted to working hours';
      }
      
      // STEP 2: Check if target day is a working day
      let safetyCounter = 0;
      while (safetyCounter < 10) {
        const dayOfWeek = targetInZone.day(); // 0 = Sunday, 6 = Saturday
        const dateStr = targetInZone.format('YYYY-MM-DD');
        
        const isWeekend = weekendDays.includes(dayOfWeek);
        const isPausedDate = pausedDates.includes(dateStr);
        
        if (!isWeekend && !isPausedDate) {
          break; // Valid working day found
        }
        
        // Shift to next day
        targetInZone.add(1, 'day').hour(businessHoursStart).minute(0).second(0);
        wasShifted = true;
        shiftReason = isWeekend ? 'Shifted past weekend' : 'Shifted past paused date';
        safetyCounter++;
      }
      
      // STEP 3: Check slot availability (rate limits)
      const slotResult = await this.findAvailableSlot(
        targetInZone.toDate(),
        settings.rateLimitEmailsPerWindow || 2,
        settings.rateLimitWindowMinutes || 15,
        leadTimezone
      );
      
      if (slotResult.shifted) {
        wasShifted = true;
        shiftReason = `Slot unavailable, shifted by ${slotResult.minutesShifted} minutes`;
      }
      
      return {
        time: slotResult.time,
        wasShifted,
        shiftReason,
        details: {
          originalTarget: moment(baseTime).add(delayHours, 'hours').toDate(),
          finalTarget: slotResult.time,
          timezone: leadTimezone
        }
      };
    } catch (error) {
      console.error('[SmartDelayService] Error calculating next valid time:', error);
      // Fallback: just add the delay
      return {
        time: moment(baseTime).add(delayHours, 'hours').toDate(),
        wasShifted: false,
        shiftReason: 'Error in calculation, used simple delay'
      };
    }
  }
  
  /**
   * Find an available slot considering rate limits
   * Checks if the time slot has capacity for another email
   * 
   * @param {Date} targetTime - Proposed schedule time
   * @param {number} maxEmailsPerWindow - Rate limit: max emails per window
   * @param {number} windowMinutes - Rate limit window in minutes
   * @param {string} timezone - Timezone for calculation
   * @returns {Promise<{time: Date, shifted: boolean, minutesShifted: number}>}
   */
  async findAvailableSlot(targetTime, maxEmailsPerWindow, windowMinutes, timezone) {
    const windowStart = moment(targetTime).subtract(windowMinutes / 2, 'minutes').toDate();
    const windowEnd = moment(targetTime).add(windowMinutes / 2, 'minutes').toDate();
    
    // Count existing scheduled emails in this window
    const existingCount = await prisma.emailJob.count({
      where: {
        scheduledFor: {
          gte: windowStart,
          lte: windowEnd
        },
        status: { in: RulebookService.getActiveStatuses() }
      }
    });
    
    if (existingCount < maxEmailsPerWindow) {
      // Slot available
      return { time: targetTime, shifted: false, minutesShifted: 0 };
    }
    
    // Slot full, find next available by shifting forward
    let shiftMinutes = windowMinutes;
    let safetyCounter = 0;
    
    while (safetyCounter < 24) { // Max 24 shifts (~6-8 hours depending on window)
      const shiftedTime = moment(targetTime).add(shiftMinutes, 'minutes').toDate();
      const newWindowStart = moment(shiftedTime).subtract(windowMinutes / 2, 'minutes').toDate();
      const newWindowEnd = moment(shiftedTime).add(windowMinutes / 2, 'minutes').toDate();
      
      const newCount = await prisma.emailJob.count({
        where: {
          scheduledFor: {
            gte: newWindowStart,
            lte: newWindowEnd
          },
          status: { in: RulebookService.getActiveStatuses() }
        }
      });
      
      if (newCount < maxEmailsPerWindow) {
        return { time: shiftedTime, shifted: true, minutesShifted: shiftMinutes };
      }
      
      shiftMinutes += windowMinutes;
      safetyCounter++;
    }
    
    // No slot found within limit, just use the shifted time
    return { time: moment(targetTime).add(shiftMinutes, 'minutes').toDate(), shifted: true, minutesShifted: shiftMinutes };
  }
  
  /**
   * Check if a given time is within working hours
   * @param {Date} time - Time to check
   * @param {string} timezone - Timezone
   * @returns {Promise<boolean>}
   */
  async isWorkingHour(time, timezone = 'UTC') {
    const settings = await SettingsRepository.getSettings();
    const hour = moment(time).tz(timezone).hour();
    
    return hour >= (settings.businessHoursStart || 9) && 
           hour < (settings.businessHoursEnd || 18);
  }
  
  /**
   * Check if a given day is a working day
   * @param {Date} time - Time to check
   * @returns {Promise<boolean>}
   */
  async isWorkingDay(time) {
    const settings = await SettingsRepository.getSettings();
    const dayOfWeek = moment(time).day();
    const dateStr = moment(time).format('YYYY-MM-DD');
    
    const weekendDays = settings.weekendDays || [0, 6];
    const pausedDates = (settings.pausedDates || []).map(d => moment(d).format('YYYY-MM-DD'));
    
    return !weekendDays.includes(dayOfWeek) && !pausedDates.includes(dateStr);
  }
  
  /**
   * Calculate reschedule time for soft bounce with smart delay
   * @param {Object} emailJob - The email job that bounced
   * @param {string} eventType - soft_bounce or deferred
   * @returns {Promise<Date>} The calculated reschedule time
   */
  async calculateRescheduleTime(emailJob, eventType) {
    // Get delay hours from settings
    const delayHours = await RulebookService.getRetryDelayHours();
    
    // Get lead timezone
    const lead = await prisma.lead.findUnique({
      where: { id: emailJob.leadId },
      select: { timezone: true }
    });
    
    const result = await this.calculateNextValidTime(
      new Date(),
      delayHours,
      lead?.timezone || 'UTC'
    );
    
    console.log(`[SmartDelayService] Calculated reschedule for job ${emailJob.id}: ${result.time} (shifted: ${result.wasShifted}, reason: ${result.shiftReason})`);
    
    return result.time;
  }
  
  /**
   * Calculate retry time for manual retry with smart delay
   * @param {Object} emailJob - The email job to retry
   * @returns {Promise<Date>} The calculated retry time
   */
  async calculateRetryTime(emailJob) {
    return this.calculateRescheduleTime(emailJob, 'manual_retry');
  }
}

module.exports = new SmartDelayService();
