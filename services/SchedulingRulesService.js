// services/SchedulingRulesService.js
// Central orchestrator for all scheduling rules
// Combines BusinessHours, WorkingDays, SmartSendTime, RateLimit, and UniqueJourney services

const moment = require('moment-timezone');
const BusinessHoursService = require('./BusinessHoursService');
const WorkingDaysService = require('./WorkingDaysService');
const SmartSendTimeService = require('./SmartSendTimeService');
const RateLimitService = require('./RateLimitService');
const UniqueJourneyService = require('./UniqueJourneyService');
const { SettingsRepository, LeadRepository } = require('../repositories');

class SchedulingRulesService {
  
  /**
   * MAIN ENTRY: Get a fully validated scheduling time applying ALL rules
   * @param {number} leadId - Lead ID
   * @param {Date|string} requestedTime - Requested scheduling time
   * @param {string} emailType - Email type to schedule
   * @returns {Promise<Object>} { success, scheduledTime, adjustments[], error? }
   */
  async getValidScheduleTime(leadId, requestedTime, emailType) {
    const adjustments = [];
    
    try {
      const lead = await LeadRepository.findById(leadId);
      if (!lead) {
        return { success: false, error: `Lead ${leadId} not found` };
      }
      
      const settings = await SettingsRepository.getSettings();
      let time = moment(requestedTime);
      
      // ============================================
      // RULE 1: Must be a working day
      // ============================================
      const workingDayCheck = WorkingDaysService.isWorkingDay(time, settings);
      if (!workingDayCheck.isWorkingDay) {
        const newTime = WorkingDaysService.getNextWorkingDay(time, settings, settings.businessHours?.startHour || 8);
        adjustments.push({
          rule: 'working_day',
          from: time.toISOString(),
          to: moment(newTime).toISOString(),
          reason: workingDayCheck.reason
        });
        time = moment(newTime);
      }
      
      // ============================================
      // RULE 2: Must be within business hours
      // ============================================
      const businessHoursCheck = BusinessHoursService.isWithinBusinessHours(time.toDate(), lead.timezone, settings);
      if (!businessHoursCheck.valid) {
        const newTime = BusinessHoursService.getNextValidSlot(time.toDate(), lead.timezone, settings);
        adjustments.push({
          rule: 'business_hours',
          from: time.toISOString(),
          to: moment(newTime).toISOString(),
          reason: businessHoursCheck.message
        });
        time = moment(newTime);
      }
      
      // ============================================
      // RULE 3: Optimize for smart send time (if enabled)
      // ============================================
      if (SmartSendTimeService.isEnabled(settings)) {
        const windowCheck = SmartSendTimeService.isInSmartWindow(time.toDate(), lead.timezone, settings);
        if (!windowCheck.inWindow) {
          const optimizedTime = SmartSendTimeService.optimizeTime(time.toDate(), lead.timezone, settings);
          if (optimizedTime.getTime() !== time.valueOf()) {
            adjustments.push({
              rule: 'smart_send_time',
              from: time.toISOString(),
              to: moment(optimizedTime).toISOString(),
              reason: 'Optimized to smart send window'
            });
            time = moment(optimizedTime);
          }
        }
      }
      
      // ============================================
      // RULE 4: Check rate limit slot availability
      // ============================================
      const reservation = await RateLimitService.reserveSlot(lead.timezone, time.toDate());
      if (reservation.success) {
        if (reservation.reservedTime.getTime() !== time.valueOf()) {
          adjustments.push({
            rule: 'rate_limit',
            from: time.toISOString(),
            to: moment(reservation.reservedTime).toISOString(),
            reason: 'Adjusted for rate limit slot'
          });
        }
        time = moment(reservation.reservedTime);
      } else {
        // Try next window
        time = moment(reservation.nextWindow);
        adjustments.push({
          rule: 'rate_limit',
          from: requestedTime.toString(),
          to: time.toISOString(),
          reason: 'Moved to next available rate limit window'
        });
      }
      
      return {
        success: true,
        scheduledTime: time.toDate(),
        adjustments,
        lead: { id: lead.id, email: lead.email, timezone: lead.timezone }
      };
      
    } catch (error) {
      console.error('[SchedulingRulesService] Error:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Validate a time without making adjustments (for user-initiated reschedules)
   * Returns all validation issues
   * @param {number} leadId 
   * @param {Date|string} requestedTime 
   * @returns {Promise<Object>} { valid, issues[] }
   */
  async validateScheduleTime(leadId, requestedTime) {
    const issues = [];
    
    try {
      const lead = await LeadRepository.findById(leadId);
      if (!lead) {
        return { valid: false, issues: [{ type: 'lead', message: 'Lead not found' }] };
      }
      
      const settings = await SettingsRepository.getSettings();
      const time = moment(requestedTime);
      
      // Check working day
      const workingDayCheck = WorkingDaysService.isWorkingDay(time, settings);
      if (!workingDayCheck.isWorkingDay) {
        issues.push({
          type: 'working_day',
          message: workingDayCheck.reason,
          severity: 'error'
        });
      }
      
      // Check business hours
      const businessHoursCheck = BusinessHoursService.isWithinBusinessHours(time.toDate(), lead.timezone, settings);
      if (!businessHoursCheck.valid) {
        issues.push({
          type: 'business_hours',
          message: businessHoursCheck.message,
          severity: 'error',
          details: {
            localTime: businessHoursCheck.localTime,
            timezone: lead.timezone,
            businessStart: businessHoursCheck.businessStart,
            businessEnd: businessHoursCheck.businessEnd
          }
        });
      }
      
      // Check smart send time (warning only)
      if (SmartSendTimeService.isEnabled(settings)) {
        const windowCheck = SmartSendTimeService.isInSmartWindow(time.toDate(), lead.timezone, settings);
        if (!windowCheck.inWindow) {
          issues.push({
            type: 'smart_send_time',
            message: 'Time is outside optimal smart send windows',
            severity: 'warning'
          });
        }
      }
      
      return {
        valid: issues.filter(i => i.severity === 'error').length === 0,
        issues,
        lead: { id: lead.id, email: lead.email, timezone: lead.timezone }
      };
      
    } catch (error) {
      return { valid: false, issues: [{ type: 'system', message: error.message }] };
    }
  }
  
  /**
   * Check if scheduling is allowed for a lead+type
   * Combines duplicate prevention with other checks
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<Object>} { allowed, reason? }
   */
  async canScheduleEmail(leadId, emailType) {
    // Check for duplicates first
    const duplicateCheck = await UniqueJourneyService.canSchedule(leadId, emailType);
    if (!duplicateCheck.allowed) {
      return duplicateCheck;
    }
    
    // Additional checks can be added here
    // (e.g., lead status, followups paused, etc.)
    
    return { allowed: true };
  }
  
  /**
   * Get scheduling constraints for UI display
   * @param {Object} settings 
   * @returns {Object} Constraints info for frontend
   */
  getSchedulingConstraints(settings) {
    return {
      businessHours: BusinessHoursService.getBusinessHoursDisplay(settings),
      weekendDays: WorkingDaysService.getWeekendDays(settings),
      smartSendTime: SmartSendTimeService.isEnabled(settings) 
        ? SmartSendTimeService.getWindows(settings) 
        : null,
      rateLimit: {
        emailsPerWindow: settings?.rateLimit?.emailsPerWindow || 2,
        windowMinutes: settings?.rateLimit?.windowMinutes || 15
      }
    };
  }
}

module.exports = new SchedulingRulesService();
