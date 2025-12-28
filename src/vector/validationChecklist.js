// Comprehensive validation checklist for backend rendering stabilization
// Ensures all system constitution requirements are met

import { A4_WIDTH, A4_HEIGHT, SAFE_MARGIN } from './constants.js';
import { validateVectorMetadata } from './validation.js';

export class ValidationChecklist {
  constructor() {
    this.results = [];
  }

  // Main validation entry point
  async validateStabilization(metadata) {
    this.results = [];
    
    try {
      // 1. Basic metadata validation
      this.validateBasicMetadata(metadata);
      
      // 2. Object-relative series coordinates
      this.validateObjectRelativeSeries(metadata);
      
      // 3. Reference-only clipping (no visual clipping)
      this.validateReferenceOnlyClipping(metadata);
      
      // 4. Original color preservation
      this.validateColorPreservation(metadata);
      
      // 5. Deterministic rendering requirements
      this.validateDeterministicRendering(metadata);
      
      // 6. Layout consistency across pages
      this.validateLayoutConsistency(metadata);
      
      return {
        isValid: this.results.every(r => r.passed),
        results: this.results,
        summary: this.generateSummary()
      };
    } catch (error) {
      this.results.push({
        test: 'Validation Engine',
        passed: false,
        error: error.message,
        severity: 'critical'
      });
      
      return {
        isValid: false,
        results: this.results,
        summary: this.generateSummary()
      };
    }
  }

  validateBasicMetadata(metadata) {
    const basicValidation = validateVectorMetadata(metadata);
    
    this.results.push({
      test: 'Basic Metadata Validation',
      passed: basicValidation.isValid,
      details: basicValidation.errors,
      severity: basicValidation.isValid ? 'info' : 'critical'
    });
  }

  validateObjectRelativeSeries(metadata) {
    const { series, ticketCrop } = metadata;
    let passed = true;
    const details = [];
    
    if (!Array.isArray(series)) {
      passed = false;
      details.push('Series must be an array');
    } else {
      series.forEach((seriesConfig, index) => {
        if (!Array.isArray(seriesConfig.slots)) {
          passed = false;
          details.push(`Series ${index}: slots must be an array`);
        } else {
          seriesConfig.slots.forEach((slot, slotIndex) => {
            // Slot coordinates are expected to be object-relative ratios (typically 0..1).
            // Allow some slack for advanced positioning but flag extreme values.
            if (Math.abs(slot.xRatio) > 2) {
              details.push(`Series ${index} slot ${slotIndex}: x ratio offset is extreme`);
            }
            if (Math.abs(slot.yRatio) > 2) {
              details.push(`Series ${index} slot ${slotIndex}: y ratio offset is extreme`);
            }
          });
        }
      });
    }
    
    this.results.push({
      test: 'Object-Relative Series Coordinates',
      passed,
      details,
      severity: passed ? 'info' : 'warning'
    });
  }

  validateReferenceOnlyClipping(metadata) {
    const { ticketCrop } = metadata;
    let passed = true;
    const details = [];
    
    // Crop is ratio-only (0..1) relative to source PDF page.
    // We do NOT validate against A4 here.
    if (Number.isFinite(ticketCrop.xRatio) && ticketCrop.xRatio < 0.01) {
      details.push('Ticket crop xRatio is very close to 0');
    }
    if (Number.isFinite(ticketCrop.yRatio) && ticketCrop.yRatio < 0.01) {
      details.push('Ticket crop yRatio is very close to 0');
    }

    const aspectRatio = Number(ticketCrop.heightRatio) > 0
      ? Number(ticketCrop.widthRatio) / Number(ticketCrop.heightRatio)
      : NaN;
    if (Number.isFinite(aspectRatio) && (aspectRatio > 10 || aspectRatio < 0.1)) {
      details.push('Extreme crop aspect ratio may cause rendering issues');
    }
    
    this.results.push({
      test: 'Reference-Only Clipping',
      passed,
      details,
      severity: passed ? 'info' : 'warning'
    });
  }

  validateColorPreservation(metadata) {
    const { series, watermarks } = metadata;
    let passed = true;
    const details = [];
    
    // Check series colors
    series.forEach((seriesConfig, index) => {
      if (seriesConfig.color !== undefined) {
        if (typeof seriesConfig.color !== 'string') {
          passed = false;
          details.push(`Series ${index}: color must be a string`);
        } else if (!seriesConfig.color.match(/^(#[0-9A-Fa-f]{3}|#[0-9A-Fa-f]{6}|rgb\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)|[a-zA-Z]+)$/)) {
          passed = false;
          details.push(`Series ${index}: invalid color format`);
        }
      }
    });
    
    // Check watermark colors
    watermarks.forEach((watermark, index) => {
      if (watermark.color !== undefined) {
        if (typeof watermark.color !== 'string') {
          passed = false;
          details.push(`Watermark ${index}: color must be a string`);
        } else if (!watermark.color.match(/^(#[0-9A-Fa-f]{3}|#[0-9A-Fa-f]{6}|rgb\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)|[a-zA-Z]+)$/)) {
          passed = false;
          details.push(`Watermark ${index}: invalid color format`);
        }
      }
    });
    
    this.results.push({
      test: 'Original Color Preservation',
      passed,
      details,
      severity: passed ? 'info' : 'warning'
    });
  }

  validateDeterministicRendering(metadata) {
    const { layout, series } = metadata;
    let passed = true;
    const details = [];
    
    // Check layout parameters for determinism
    if (layout.repeatPerPage < 1 || layout.repeatPerPage > 16) {
      passed = false;
      details.push('repeatPerPage must be between 1 and 16 for deterministic rendering');
    }
    
    if (layout.totalPages < 1 || layout.totalPages > 100000) {
      passed = false;
      details.push('totalPages must be reasonable for deterministic rendering');
    }
    
    // Check series progression determinism
    series.forEach((seriesConfig, index) => {
      if (seriesConfig.step <= 0) {
        passed = false;
        details.push(`Series ${index}: step must be positive for deterministic progression`);
      }
      
      if (seriesConfig.start < 0) {
        details.push(`Series ${index}: negative start value may cause issues`);
      }
    });
    
    this.results.push({
      test: 'Deterministic Rendering',
      passed,
      details,
      severity: passed ? 'info' : 'warning'
    });
  }

  validateLayoutConsistency(metadata) {
    const { layout, series, ticketCrop } = metadata;
    let passed = true;
    const details = [];
    
    // Check series slots are compatible with repeatPerPage
    // slots may be length 1 (broadcast) or length == repeatPerPage.
    const repeatPerPage = Number(layout?.repeatPerPage || 4);
    series.forEach((seriesConfig, index) => {
      const slotCount = Array.isArray(seriesConfig?.slots) ? seriesConfig.slots.length : 0;
      if (slotCount !== 1 && slotCount !== repeatPerPage) {
        passed = false;
        details.push(`Series ${index}: slots length (${slotCount}) must be 1 or repeatPerPage (${repeatPerPage})`);
      }
    });
    
    // Check object positioning consistency
    const objectRatio = Number(ticketCrop.widthRatio) * Number(ticketCrop.heightRatio);
    
    if (objectRatio > 0.8) {
      details.push('Object occupies most of page - may cause layout issues');
    }
    
    if (objectRatio < 0.01) {
      details.push('Object very small relative to page - may be hard to position series');
    }
    
    this.results.push({
      test: 'Layout Consistency',
      passed,
      details,
      severity: passed ? 'info' : 'warning'
    });
  }

  generateSummary() {
    const critical = this.results.filter(r => r.severity === 'critical').length;
    const warnings = this.results.filter(r => r.severity === 'warning').length;
    const info = this.results.filter(r => r.severity === 'info').length;
    
    return {
      totalTests: this.results.length,
      criticalErrors: critical,
      warnings: warnings,
      infoMessages: info,
      overallStatus: critical === 0 ? 'PASS' : 'FAIL',
      recommendations: this.generateRecommendations()
    };
  }

  generateRecommendations() {
    const recommendations = [];
    
    this.results.forEach(result => {
      if (!result.passed && result.details) {
        result.details.forEach(detail => {
          recommendations.push(`- ${detail}`);
        });
      }
    });
    
    if (recommendations.length === 0) {
      recommendations.push('- All validation checks passed. System is ready for production.');
    }
    
    return recommendations;
  }
}

// Singleton instance
export const validationChecklist = new ValidationChecklist();
