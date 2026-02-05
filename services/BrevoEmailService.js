// services/BrevoEmailService.js
// Brevo email service using Prisma
// Supports MOCK_BREVO_URL env var for testing with mock server

const axios = require('axios');
const { SettingsRepository } = require('../repositories');
const { loggers } = require('../lib/logger');
const log = loggers.email;
require('dotenv').config();

class BrevoEmailService {
  constructor() {
    // Use MOCK_BREVO_URL if set, otherwise use real Brevo API
    // To use mock: set MOCK_BREVO_URL=http://localhost:3001
    this.apiUrl = process.env.MOCK_BREVO_URL 
      ? `${process.env.MOCK_BREVO_URL}/v3/smtp/email`
      : "https://api.brevo.com/v3/smtp/email";
    this._cachedCredentials = null;
    this._credentialsCacheTime = null;
    this._cacheDurationMs = 60000; // Cache for 1 minute
    
    if (process.env.MOCK_BREVO_URL) {
      log.warn({ mockUrl: process.env.MOCK_BREVO_URL }, 'MOCK MODE enabled');
    }
  }

  /**
   * Invalidate the credentials cache (call when Brevo settings are updated)
   */
  invalidateCredentialsCache() {
    this._cachedCredentials = null;
    this._credentialsCacheTime = null;
    log.debug('Credentials cache invalidated');
  }

  /**
   * Get Brevo credentials from Settings DB with fallback to .env
   */
  async getCredentials() {
    const now = Date.now();

    if (
      this._cachedCredentials &&
      this._credentialsCacheTime &&
      now - this._credentialsCacheTime < this._cacheDurationMs
    ) {
      return this._cachedCredentials;
    }

    try {
      const settings = await SettingsRepository.getSettings();
      const brevoConfig = settings.brevo || {};

      this._cachedCredentials = {
        apiKey: brevoConfig.apiKey || process.env.BREVO_API_KEY || "",
        fromEmail:
          brevoConfig.fromEmail ||
          process.env.FROM_EMAIL ||
          "noreply@example.com",
        fromName:
          brevoConfig.fromName || process.env.FROM_NAME || "Your Company",
      };
      this._credentialsCacheTime = now;

      if (!this._cachedCredentials.apiKey) {
        log.error('No Brevo API key configured in DB or .env');
      }

      return this._cachedCredentials;
    } catch (error) {
      log.error({ error: error.message }, 'Error loading Brevo credentials from DB');
      return {
        apiKey: process.env.BREVO_API_KEY || "",
        fromEmail: process.env.FROM_EMAIL || "noreply@example.com",
        fromName: process.env.FROM_NAME || "Your Company",
      };
    }
  }

  async sendEmail(emailJobOrParams, leadOrNothing) {
    const credentials = await this.getCredentials();
    const { prisma } = require("../lib/prisma");

    // Handle both calling styles:
    // 1. sendEmail(emailJob, lead) - from emailWorker
    // 2. sendEmail({ to, name, type, ... }) - old style direct call
    let to,
      name,
      type,
      idempotencyKey,
      customSubject,
      customHtml,
      templateId,
      lead;

    if (leadOrNothing) {
      // Called as sendEmail(emailJob, lead)
      const emailJob = emailJobOrParams;
      lead = leadOrNothing;
      to = emailJob.email || lead.email;
      name = lead.name || "Valued Customer";
      type = emailJob.type;
      idempotencyKey = emailJob.idempotencyKey;
      templateId = emailJob.templateId;
      customSubject = emailJob.metadata?.subject;
      customHtml = emailJob.metadata?.htmlContent;
    } else {
      // Called as sendEmail({ to, name, type, ... })
      to = emailJobOrParams.to;
      name = emailJobOrParams.name;
      type = emailJobOrParams.type;
      idempotencyKey = emailJobOrParams.idempotencyKey;
      templateId = emailJobOrParams.templateId;
      customSubject = emailJobOrParams.subject;
      customHtml = emailJobOrParams.htmlContent;
      lead = emailJobOrParams.lead || {}; // Optional lead object for variable replacement
    }

    let subject = customSubject;
    let htmlContent = customHtml;

    // If templateId is provided, fetch template from database
    if (templateId && !htmlContent) {
      try {
        console.log(`[BrevoEmailService] Fetching template ID: ${templateId}`);
        const template = await prisma.emailTemplate.findUnique({
          where: { id: parseInt(templateId) },
        });

        if (template) {
          console.log(
            `[BrevoEmailService] Found template: "${template.name}" with subject: "${template.subject}"`,
          );
          subject = template.subject || subject;
          htmlContent = template.body || template.content;

          // Build variable replacements from lead data
          const firstName = name.split(" ")[0] || "";
          const lastName = name.split(" ").slice(1).join(" ") || "";
          const company = name || name || "";
          const position = lead?.position || lead?.title || "";
          const phone = lead?.phone || "";
          const source = lead?.source || "";

          // Apply ALL personalization variables to htmlContent
          if (htmlContent) {
            htmlContent = htmlContent
              .replace(/\{\{name\}\}/gi, name)
              .replace(/\{\{firstName\}\}/gi, firstName)
              .replace(/\{\{lastName\}\}/gi, lastName)
              .replace(/\{\{email\}\}/gi, to)
              .replace(/\{\{company\}\}/gi, company)
              .replace(/\{\{companyName\}\}/gi, company)
              .replace(/\{\{position\}\}/gi, position)
              .replace(/\{\{title\}\}/gi, position)
              .replace(/\{\{phone\}\}/gi, phone)
              .replace(/\{\{source\}\}/gi, source);
          }
          // Apply variables to subject line
          if (subject) {
            subject = subject
              .replace(/\{\{name\}\}/gi, name)
              .replace(/\{\{firstName\}\}/gi, firstName)
              .replace(/\{\{lastName\}\}/gi, lastName)
              .replace(/\{\{company\}\}/gi, company);
          }
        } else {
          console.warn(
            `[BrevoEmailService] Template ID ${templateId} not found in database`,
          );
        }
      } catch (err) {
        console.error(
          `[BrevoEmailService] Failed to fetch template ${templateId}:`,
          err.message,
        );
      }
    } else if (!templateId) {
      console.log(
        `[BrevoEmailService] No templateId provided, using default template for type: ${type}`,
      );
    }

    // Fallback to default templates
    if (!subject) {
      subject = type?.toLowerCase().includes("initial")
        ? "Welcome! Let's get started"
        : "Following up on our previous message";
    }

    if (!htmlContent) {
      htmlContent = type?.toLowerCase().includes("initial")
        ? this.getInitialEmailTemplate(name)
        : this.getFollowUpEmailTemplate(name);
    }

    try {
      log.debug({ subject, to }, 'Sending email via Brevo');
      
      const response = await axios.post(
        this.apiUrl,
        {
          sender: { email: credentials.fromEmail, name: credentials.fromName },
          to: [{ email: to, name }],
          subject,
          htmlContent,
          headers: {
            "X-Idempotency-Key": idempotencyKey,
          },
        },
        {
          headers: {
            "api-key": credentials.apiKey,
            "Content-Type": "application/json",
          },
        },
      );

      return {
        success: true,
        messageId: response.data.messageId,
      };
    } catch (error) {
      throw new Error(
        `Brevo API error: ${error.response?.data?.message || error.message}`,
      );
    }
  }

  getInitialEmailTemplate(name) {
    return `
      <html>
        <body>
          <h2>Hi ${name},</h2>
          <p>Welcome to our platform! We're excited to have you here.</p>
          <p>This is your initial welcome email.</p>
          <p>Best regards,<br>The Team</p>
        </body>
      </html>
    `;
  }

  getFollowUpEmailTemplate(name) {
    return `
      <html>
        <body>
          <h2>Hi ${name},</h2>
          <p>Just following up on our previous message.</p>
          <p>We'd love to hear from you!</p>
          <p>Best regards,<br>The Team</p>
        </body>
      </html>
    `;
  }
}

module.exports = new BrevoEmailService();