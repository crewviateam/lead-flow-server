const express = require('express');
const router = express.Router();
const TemplateController = require('../controllers/TemplateController');

router.get('/', TemplateController.getAllTemplates);
router.get('/:id', TemplateController.getTemplate);
router.post('/', TemplateController.createTemplate);
router.put('/:id', TemplateController.updateTemplate);
router.delete('/:id', TemplateController.deleteTemplate);

module.exports = router;
