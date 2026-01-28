// repositories/index.js
// Central export for all repositories

const LeadRepository = require('./LeadRepository');
const EmailJobRepository = require('./EmailJobRepository');
const SettingsRepository = require('./SettingsRepository');
const TemplateRepository = require('./TemplateRepository');
const EventStoreRepository = require('./EventStoreRepository');
const NotificationRepository = require('./NotificationRepository');

module.exports = {
  LeadRepository,
  EmailJobRepository,
  SettingsRepository,
  TemplateRepository,
  EventStoreRepository,
  NotificationRepository
};
