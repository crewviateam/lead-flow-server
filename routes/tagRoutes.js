// routes/tagRoutes.js
// Tag management routes (bulk operations)

const express = require('express');
const router = express.Router();
const TagController = require('../controllers/TagController');

router.get('/', TagController.getAllTags.bind(TagController));
router.post('/bulk-add', TagController.bulkAddTags.bind(TagController));
router.post('/bulk-remove', TagController.bulkRemoveTags.bind(TagController));

module.exports = router;
