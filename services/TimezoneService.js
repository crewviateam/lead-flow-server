// services/TimezoneService.js
const ct = require('countries-and-timezones');

class TimezoneService {
  getTimezone(country, city) {
    // Get country timezones
    const countryData = ct.getCountry(country);
    if (!countryData || !countryData.timezones) {
      return null;
    }

    // If country has only one timezone, return it
    if (countryData.timezones.length === 1) {
      return countryData.timezones[0];
    }

    // For countries with multiple timezones, use city mapping
    const cityTimezoneMap = this.getCityTimezoneMap();
    const cityKey = `${country.toLowerCase()}_${city.toLowerCase()}`;
    
    if (cityTimezoneMap[cityKey]) {
      return cityTimezoneMap[cityKey];
    }

    // Default to first timezone if no specific mapping
    return countryData.timezones[0];
  }

  getCityTimezoneMap() {
    // Pre-defined mappings for major cities in multi-timezone countries
    return {
      'us_new york': 'America/New_York',
      'us_los angeles': 'America/Los_Angeles',
      'us_chicago': 'America/Chicago',
      'us_denver': 'America/Denver',
      'us_phoenix': 'America/Phoenix',
      'ca_toronto': 'America/Toronto',
      'ca_vancouver': 'America/Vancouver',
      'au_sydney': 'Australia/Sydney',
      'au_melbourne': 'Australia/Melbourne',
      'au_perth': 'Australia/Perth',
      'br_sao paulo': 'America/Sao_Paulo',
      'br_manaus': 'America/Manaus',
      'ru_moscow': 'Europe/Moscow',
      'ru_vladivostok': 'Asia/Vladivostok',
      'mx_mexico city': 'America/Mexico_City',
      'mx_tijuana': 'America/Tijuana'
    };
  }

  isBusinessHours(date, timezone, businessHours = { startHour: 8, endHour: 22 }) {
    const moment = require('moment-timezone');
    const localTime = moment(date).tz(timezone);
    const hour = localTime.hour();
    
    // Business hours from settings (default 8 AM to 10 PM)
    return hour >= businessHours.startHour && hour < businessHours.endHour;
  }

  getNextBusinessHourSlot(timezone, fromTime = new Date(), businessHours = { startHour: 8, endHour: 22 }, windowMinutes = 15) {
    const moment = require('moment-timezone');
    let nextSlot = moment(fromTime).tz(timezone);

    // If current time is past endHour, move to next day startHour
    if (nextSlot.hour() >= businessHours.endHour) {
      nextSlot.add(1, 'day').hour(businessHours.startHour).minute(0).second(0);
    } 
    // If before startHour, set to startHour today
    else if (nextSlot.hour() < businessHours.startHour) {
      nextSlot.hour(businessHours.startHour).minute(0).second(0);
    }
    // Otherwise, use current time rounded to next window slot
    else {
      const minutes = nextSlot.minute();
      const roundedMinutes = Math.ceil(minutes / windowMinutes) * windowMinutes;
      nextSlot.minute(roundedMinutes).second(0);
      
      if (roundedMinutes >= 60) {
        nextSlot.add(1, 'hour').minute(0);
      }
      
      // If rounding pushed us past endHour, move to next day
      if (nextSlot.hour() >= businessHours.endHour) {
        nextSlot.add(1, 'day').hour(businessHours.startHour).minute(0).second(0);
      }
    }

    return nextSlot.toDate();
  }
}

module.exports = new TimezoneService();