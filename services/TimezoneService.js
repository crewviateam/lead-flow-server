// services/TimezoneService.js
const ct = require('countries-and-timezones');

class TimezoneService {
  // Normalize country codes (e.g., UK -> GB)
  normalizeCountryCode(country) {
    const countryNormalization = {
      uk: "GB",
      "united kingdom": "GB",
      "great britain": "GB",
      england: "GB",
      scotland: "GB",
      wales: "GB",
      usa: "US",
      "united states": "US",
      america: "US",
    };

    const normalized = country.trim().toLowerCase();
    return countryNormalization[normalized] || country.toUpperCase();
  }

  getTimezone(country, city) {
    // Normalize country code first
    const normalizedCountry = this.normalizeCountryCode(country);

    // Get country timezones
    const countryData = ct.getCountry(normalizedCountry);
    if (!countryData || !countryData.timezones) {
      // Fallback: Check if it's a known single-timezone country
      const fallbackTimezones = {
        GB: "Europe/London",
        UK: "Europe/London",
        DE: "Europe/Berlin",
        FR: "Europe/Paris",
        IT: "Europe/Rome",
        ES: "Europe/Madrid",
        NL: "Europe/Amsterdam",
        BE: "Europe/Brussels",
        CH: "Europe/Zurich",
        AT: "Europe/Vienna",
        SE: "Europe/Stockholm",
        NO: "Europe/Oslo",
        DK: "Europe/Copenhagen",
        FI: "Europe/Helsinki",
        IE: "Europe/Dublin",
        PL: "Europe/Warsaw",
        IN: "Asia/Kolkata",
        SG: "Asia/Singapore",
        JP: "Asia/Tokyo",
        KR: "Asia/Seoul",
        CN: "Asia/Shanghai",
        HK: "Asia/Hong_Kong",
        NZ: "Pacific/Auckland",
        ZA: "Africa/Johannesburg",
        AE: "Asia/Dubai",
        IL: "Asia/Jerusalem",
      };
      return fallbackTimezones[normalizedCountry] || null;
    }

    // If country has only one timezone, return it
    if (countryData.timezones.length === 1) {
      return countryData.timezones[0];
    }

    // For countries with multiple timezones, use city mapping
    const cityTimezoneMap = this.getCityTimezoneMap();
    const cityKey = `${normalizedCountry.toLowerCase()}_${city.toLowerCase()}`;

    if (cityTimezoneMap[cityKey]) {
      return cityTimezoneMap[cityKey];
    }

    // Default to first timezone if no specific mapping
    return countryData.timezones[0];
  }

  getCityTimezoneMap() {
    // Pre-defined mappings for major cities in multi-timezone countries
    return {
      // UK cities (all use Europe/London)
      gb_london: "Europe/London",
      gb_manchester: "Europe/London",
      gb_birmingham: "Europe/London",
      gb_leeds: "Europe/London",
      gb_glasgow: "Europe/London",
      gb_edinburgh: "Europe/London",
      gb_liverpool: "Europe/London",
      gb_bristol: "Europe/London",
      gb_sheffield: "Europe/London",
      gb_newcastle: "Europe/London",
      gb_nottingham: "Europe/London",
      gb_southampton: "Europe/London",
      gb_portsmouth: "Europe/London",
      gb_brighton: "Europe/London",
      gb_leicester: "Europe/London",
      gb_coventry: "Europe/London",
      gb_cardiff: "Europe/London",
      gb_belfast: "Europe/London",
      gb_oxford: "Europe/London",
      gb_cambridge: "Europe/London",
      gb_york: "Europe/London",
      gb_bath: "Europe/London",
      gb_reading: "Europe/London",
      gb_marylebone: "Europe/London",
      gb_weyside: "Europe/London",
      // US cities
      "us_new york": "America/New_York",
      "us_los angeles": "America/Los_Angeles",
      us_chicago: "America/Chicago",
      us_denver: "America/Denver",
      us_phoenix: "America/Phoenix",
      us_houston: "America/Chicago",
      us_dallas: "America/Chicago",
      "us_san francisco": "America/Los_Angeles",
      us_seattle: "America/Los_Angeles",
      us_boston: "America/New_York",
      us_miami: "America/New_York",
      us_atlanta: "America/New_York",
      // Canada cities
      ca_toronto: "America/Toronto",
      ca_vancouver: "America/Vancouver",
      ca_montreal: "America/Toronto",
      ca_calgary: "America/Edmonton",
      // Australia cities
      au_sydney: "Australia/Sydney",
      au_melbourne: "Australia/Melbourne",
      au_perth: "Australia/Perth",
      au_brisbane: "Australia/Brisbane",
      au_adelaide: "Australia/Adelaide",
      // Other countries
      "br_sao paulo": "America/Sao_Paulo",
      br_manaus: "America/Manaus",
      ru_moscow: "Europe/Moscow",
      ru_vladivostok: "Asia/Vladivostok",
      "ru_saint petersburg": "Europe/Moscow",
      "mx_mexico city": "America/Mexico_City",
      mx_tijuana: "America/Tijuana",
    };
  }

  isBusinessHours(
    date,
    timezone,
    businessHours = { startHour: 8, endHour: 22 },
  ) {
    const moment = require("moment-timezone");
    const localTime = moment(date).tz(timezone);
    const hour = localTime.hour();

    // Business hours from settings (default 8 AM to 10 PM)
    return hour >= businessHours.startHour && hour < businessHours.endHour;
  }

  getNextBusinessHourSlot(
    timezone,
    fromTime = new Date(),
    businessHours = { startHour: 8, endHour: 22 },
    windowMinutes = 15,
  ) {
    const moment = require("moment-timezone");
    let nextSlot = moment(fromTime).tz(timezone);

    // If current time is past endHour, move to next day startHour
    if (nextSlot.hour() >= businessHours.endHour) {
      nextSlot.add(1, "day").hour(businessHours.startHour).minute(0).second(0);
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
        nextSlot.add(1, "hour").minute(0);
      }

      // If rounding pushed us past endHour, move to next day
      if (nextSlot.hour() >= businessHours.endHour) {
        nextSlot
          .add(1, "day")
          .hour(businessHours.startHour)
          .minute(0)
          .second(0);
      }
    }

    return nextSlot.toDate();
  }
}

module.exports = new TimezoneService();