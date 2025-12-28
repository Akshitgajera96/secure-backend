import { A4_HEIGHT } from './constants.js';

// Centralized coordinate conversion utilities
export class CoordinateConverter {
  constructor(scale = 1.0) {
    this.scale = scale;
  }

  // Convert canvas coordinates to PDF points
  canvasToPdf(canvasX, canvasY, canvasWidth = 0, canvasHeight = 0) {
    return {
      pdfX: canvasX / this.scale,
      pdfY: A4_HEIGHT - (canvasY + canvasHeight) / this.scale
    };
  }

  // Convert PDF points to canvas coordinates
  pdfToCanvas(pdfX, pdfY) {
    return {
      canvasX: pdfX * this.scale,
      canvasY: (A4_HEIGHT - pdfY) * this.scale
    };
  }

  // Snap to PDF point precision
  snap(value) {
    return Math.round(value * 1000) / 1000;
  }

  // Apply calibration offset
  applyCalibration(x, y, calibration = { dx: 0, dy: 0 }) {
    return {
      x: this.snap(x + calibration.dx),
      y: this.snap(y + calibration.dy)
    };
  }
}

// Singleton instance for reuse
export const coordinateConverter = new CoordinateConverter();
