// services/LeadImportService.js
// Lead import service using Prisma

const Papa = require('papaparse');
const XLSX = require('xlsx');
const { LeadRepository } = require('../repositories');
const TimezoneService = require('./TimezoneService');
const EventBus = require('../events/EventBus');

class LeadImportService {
  async parseFile(fileBuffer, fileType) {
    if (fileType === 'csv') {
      return this.parseCSV(fileBuffer);
    } else if (fileType === 'xlsx') {
      return this.parseXLSX(fileBuffer);
    }
    throw new Error('Unsupported file type');
  }

  parseCSV(fileBuffer) {
    const csvString = fileBuffer.toString('utf-8');
    const result = Papa.parse(csvString, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (header) => header.trim().toLowerCase()
    });

    return result.data;
  }

  parseXLSX(fileBuffer) {
    const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(sheet);

    // Normalize headers to lowercase
    return data.map(row => {
      const normalized = {};
      for (const key in row) {
        normalized[key.toLowerCase().trim()] = row[key];
      }
      return normalized;
    });
  }

  validateLead(row) {
    const errors = [];
    
    if (!row.email || !this.isValidEmail(row.email)) {
      errors.push('Invalid email');
    }
    if (!row.name || row.name.trim().length === 0) {
      errors.push('Name is required');
    }
    if (!row.country || row.country.trim().length === 0) {
      errors.push('Country is required');
    }
    if (!row.city || row.city.trim().length === 0) {
      errors.push('City is required');
    }

    return errors.length > 0 ? errors : null;
  }

  isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  async importLeads(fileBuffer, fileType) {
    const rows = await this.parseFile(fileBuffer, fileType);
    const results = { success: 0, failed: 0, errors: [] };

    const createdLeads = [];

    for (const row of rows) {
      try {
        const validationErrors = this.validateLead(row);
        if (validationErrors) {
          results.failed++;
          results.errors.push({ row, errors: validationErrors });
          continue;
        }

        // Determine timezone
        const timezone = TimezoneService.getTimezone(row.country, row.city);
        if (!timezone) {
          results.failed++;
          results.errors.push({ 
            row, 
            errors: [`Could not determine timezone for ${row.city}, ${row.country}`] 
          });
          continue;
        }

        // Check if lead already exists
        const existingLead = await LeadRepository.findByEmail(row.email);
        if (existingLead) {
          results.failed++;
          results.errors.push({ row, errors: ['Email already exists'] });
          continue;
        }

        // Create lead with Prisma
        const lead = await LeadRepository.create({
          email: row.email.toLowerCase().trim(),
          name: row.name.trim(),
          country: row.country.trim(),
          city: row.city.trim(),
          timezone,
          status: 'pending'
        });

        createdLeads.push(lead);

        // Emit LeadCreated event
        await EventBus.emit('LeadCreated', {
          leadId: lead.id.toString(),
          email: lead.email,
          name: lead.name,
          country: lead.country,
          city: lead.city,
          timezone: lead.timezone
        });

        results.success++;
      } catch (error) {
        results.failed++;
        results.errors.push({ row, errors: [error.message] });
      }
    }

    results.leads = createdLeads; // Return created leads
    return results;
  }
}

module.exports = new LeadImportService();