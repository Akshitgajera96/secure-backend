// Vector data contract validation - MUST MATCH EXACTLY
 

export const validateVectorMetadata = (metadata) => {
  const errors = [];
  
  // Validate required top-level fields
  if (!metadata.sourcePdfKey || typeof metadata.sourcePdfKey !== 'string') {
    errors.push('sourcePdfKey is required and must be a string');
  }
  
  if (!metadata.ticketCrop || typeof metadata.ticketCrop !== 'object') {
    errors.push('ticketCrop is required and must be an object');
  } else {
    validateTicketCrop(metadata.ticketCrop, errors);
  }
  
  if (!metadata.layout || typeof metadata.layout !== 'object') {
    errors.push('layout is required and must be an object');
  } else {
    validateLayout(metadata.layout, errors);
  }
  
  if (!Array.isArray(metadata.series)) {
    errors.push('series must be an array');
  } else {
    metadata.series.forEach((s, i) => validateSeries(s, i, errors, metadata.layout));
  }
  
  if (!Array.isArray(metadata.watermarks)) {
    errors.push('watermarks must be an array');
  } else {
    metadata.watermarks.forEach((w, i) => validateWatermark(w, i, errors));
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
};

const validateTicketCrop = (ticketCrop, errors) => {
  const required = ['pageIndex', 'xRatio', 'yRatio', 'widthRatio', 'heightRatio'];
  required.forEach(field => {
    if (typeof ticketCrop[field] !== 'number') {
      errors.push(`ticketCrop.${field} is required and must be a number`);
    }
  });

  // ticketCrop is defined in SOURCE PDF coordinate space.
  // It must NOT be validated against A4 bounds here.
  if (!Number.isFinite(ticketCrop.pageIndex) || ticketCrop.pageIndex < 0) {
    errors.push('ticketCrop.pageIndex must be a number >= 0');
  }
  if (!Number.isFinite(ticketCrop.xRatio) || !Number.isFinite(ticketCrop.yRatio)) {
    errors.push('ticketCrop.xRatio and ticketCrop.yRatio must be finite numbers');
  }
  if (!Number.isFinite(ticketCrop.widthRatio) || ticketCrop.widthRatio <= 0) {
    errors.push('ticketCrop.widthRatio must be a number > 0');
  }
  if (!Number.isFinite(ticketCrop.heightRatio) || ticketCrop.heightRatio <= 0) {
    errors.push('ticketCrop.heightRatio must be a number > 0');
  }

  if (Number.isFinite(ticketCrop.xRatio) && (ticketCrop.xRatio < 0 || ticketCrop.xRatio > 1)) {
    errors.push('ticketCrop.xRatio must be between 0 and 1');
  }
  if (Number.isFinite(ticketCrop.yRatio) && (ticketCrop.yRatio < 0 || ticketCrop.yRatio > 1)) {
    errors.push('ticketCrop.yRatio must be between 0 and 1');
  }
  if (Number.isFinite(ticketCrop.widthRatio) && ticketCrop.widthRatio > 1) {
    errors.push('ticketCrop.widthRatio must be <= 1');
  }
  if (Number.isFinite(ticketCrop.heightRatio) && ticketCrop.heightRatio > 1) {
    errors.push('ticketCrop.heightRatio must be <= 1');
  }

  const isUserMmMode =
    ticketCrop &&
    typeof ticketCrop.widthMm === 'number' &&
    typeof ticketCrop.heightMm === 'number' &&
    ticketCrop.widthMm > 0 &&
    ticketCrop.heightMm > 0;
  if (!isUserMmMode) return;

  const widthMm = ticketCrop.widthMm;
  const heightMm = ticketCrop.heightMm;
  if (typeof widthMm !== 'number' || !Number.isFinite(widthMm) || widthMm <= 0) {
    errors.push('ticketCrop.widthMm must be a finite number > 0');
  }
  if (typeof heightMm !== 'number' || !Number.isFinite(heightMm) || heightMm <= 0) {
    errors.push('ticketCrop.heightMm must be a finite number > 0');
  }

  if (ticketCrop.xMm !== undefined && ticketCrop.xMm !== null) {
    if (typeof ticketCrop.xMm !== 'number' || !Number.isFinite(ticketCrop.xMm) || ticketCrop.xMm < 0) {
      errors.push('ticketCrop.xMm must be a finite number >= 0 when provided');
    }
  }

  if (ticketCrop.yMm !== undefined && ticketCrop.yMm !== null) {
    if (typeof ticketCrop.yMm !== 'number' || !Number.isFinite(ticketCrop.yMm) || ticketCrop.yMm < 0) {
      errors.push('ticketCrop.yMm must be a finite number >= 0 when provided');
    }
  }

  if (ticketCrop.cutMarginMm !== undefined && ticketCrop.cutMarginMm !== null) {
    if (typeof ticketCrop.cutMarginMm !== 'number' || !Number.isFinite(ticketCrop.cutMarginMm) || ticketCrop.cutMarginMm < 0) {
      errors.push('ticketCrop.cutMarginMm must be a finite number >= 0 when provided');
    }
  }

  if (ticketCrop.rotationDeg !== undefined && ticketCrop.rotationDeg !== null) {
    if (typeof ticketCrop.rotationDeg !== 'number' || !Number.isFinite(ticketCrop.rotationDeg)) {
      errors.push('ticketCrop.rotationDeg must be a finite number when provided');
    }
  }

  if (ticketCrop.keepProportions !== undefined && ticketCrop.keepProportions !== null) {
    if (typeof ticketCrop.keepProportions !== 'boolean') {
      errors.push('ticketCrop.keepProportions must be a boolean when provided');
    }
  }

  if (ticketCrop.alignment !== undefined && ticketCrop.alignment !== null) {
    if (typeof ticketCrop.alignment !== 'string' || !['left', 'center', 'right'].includes(ticketCrop.alignment)) {
      errors.push('ticketCrop.alignment must be one of: left, center, right when provided');
    }
  }
};

const validateLayout = (layout, errors) => {
  if (layout.pageSize !== 'A4') {
    errors.push('layout.pageSize must be "A4"');
  }
  
  // repeatPerPage is backend-owned. If missing, backend defaults it.
  if (layout.repeatPerPage !== undefined) {
    if (typeof layout.repeatPerPage !== 'number' || layout.repeatPerPage < 1 || layout.repeatPerPage > 16) {
      errors.push('layout.repeatPerPage must be a number between 1 and 16');
    }
  }

  if (typeof layout.totalPages !== 'number' || layout.totalPages < 1 || layout.totalPages > 100000) {
    errors.push('layout.totalPages must be a positive number');
  }
};

const validateSeries = (series, index, errors, layout) => {
  if (!series.id || typeof series.id !== 'string') {
    errors.push(`series[${index}].id is required and must be a string`);
  }
  
  if (typeof series.prefix !== 'string') {
    errors.push(`series[${index}].prefix is required and must be a string`);
  }

  if (series.padLength !== undefined && typeof series.padLength !== 'number') {
    errors.push(`series[${index}].padLength must be a number when provided`);
  }
  
  if (typeof series.start !== 'number') {
    errors.push(`series[${index}].start is required and must be a number`);
  }
  
  if (typeof series.step !== 'number' || series.step < 1) {
    errors.push(`series[${index}].step is required and must be a positive number`);
  }
  
  if (!series.font || typeof series.font !== 'string') {
    errors.push(`series[${index}].font is required and must be a string`);
  }
  
  if (typeof series.fontSize !== 'number' || series.fontSize < 6 || series.fontSize > 72) {
    errors.push(`series[${index}].fontSize must be a number between 6 and 72`);
  }
  
  // Optional color field validation (preserve original colors)
  if (series.color !== undefined) {
    if (typeof series.color !== 'string') {
      errors.push(`series[${index}].color must be a string when provided`);
    } else {
      // Validate color format (hex, rgb, or named color)
      if (!series.color.match(/^(#[0-9A-Fa-f]{3}|#[0-9A-Fa-f]{6}|rgb\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)|[a-zA-Z]+)$/)) {
        errors.push(`series[${index}].color must be a valid color format (hex, rgb, or named color)`);
      }
    }
  }
  
  if (!Array.isArray(series.slots)) {
    errors.push(`series[${index}].slots must be an array`);
  } else {
    // Slots may be provided as a single object-relative slot (broadcast to all objects)
    // or as one-per-object when repeatPerPage is explicitly provided.
    if (layout && typeof layout.repeatPerPage === 'number') {
      if (series.slots.length !== 1 && series.slots.length !== layout.repeatPerPage) {
        errors.push(`series[${index}].slots length must be 1 or equal layout.repeatPerPage`);
      }
    }
    series.slots.forEach((slot, slotIndex) => {
      validateSlot(slot, `${index}.slots[${slotIndex}]`, errors);
    });
  }
};

const validateSlot = (slot, path, errors) => {
  if (typeof slot.xRatio !== 'number' || typeof slot.yRatio !== 'number') {
    errors.push(`${path}.xRatio and ${path}.yRatio must be numbers`);
  }
  
  // Series slots are now object-relative (relative to ticketCrop bbox)
  // No A4 bounds validation needed - object-relative coordinates can be positive or negative
  // This allows series to be positioned relative to the object, not the page
};

const validateWatermark = (watermark, index, errors) => {
  if (!watermark.id || typeof watermark.id !== 'string') {
    errors.push(`watermarks[${index}].id is required and must be a string`);
  }
  
  if (!['text', 'svg'].includes(watermark.type)) {
    errors.push(`watermarks[${index}].type must be "text" or "svg"`);
  }
  
  if (watermark.type === 'text') {
    if (!watermark.value || typeof watermark.value !== 'string') {
      errors.push(`watermarks[${index}].value is required for text watermarks`);
    }
  }
  
  if (watermark.type === 'svg') {
    if (!watermark.svgPath || typeof watermark.svgPath !== 'string') {
      errors.push(`watermarks[${index}].svgPath is required for SVG watermarks`);
    }
  }
  
  if (typeof watermark.opacity !== 'number' || watermark.opacity < 0 || watermark.opacity > 1) {
    errors.push(`watermarks[${index}].opacity must be a number between 0 and 1`);
  }
  
  if (typeof watermark.rotate !== 'number') {
    errors.push(`watermarks[${index}].rotate must be a number`);
  }
  
  // Optional color field validation (preserve original colors)
  if (watermark.color !== undefined) {
    if (typeof watermark.color !== 'string') {
      errors.push(`watermarks[${index}].color must be a string when provided`);
    } else {
      // Validate color format (hex, rgb, or named color)
      if (!watermark.color.match(/^(#[0-9A-Fa-f]{3}|#[0-9A-Fa-f]{6}|rgb\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)|[a-zA-Z]+)$/)) {
        errors.push(`watermarks[${index}].color must be a valid color format (hex, rgb, or named color)`);
      }
    }
  }
  
  if (!watermark.position || typeof watermark.position !== 'object') {
    errors.push(`watermarks[${index}].position is required and must be an object`);
  } else {
    if (typeof watermark.position.x !== 'number' || typeof watermark.position.y !== 'number') {
      errors.push(`watermarks[${index}].position.x and .y must be numbers`);
    }
  }
};
