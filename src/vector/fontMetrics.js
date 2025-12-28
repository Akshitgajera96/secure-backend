// Font metrics cache for baseline-aware text rendering
export class FontMetricsCache {
  constructor() {
    this.cache = new Map();
    this.fallbackLogged = new Set();
  }

  // Get font metrics for a specific font and size
  getMetrics(fontFamily, fontSize, pdfFont) {
    const key = `${fontFamily}-${fontSize}`;
    
    if (!this.cache.has(key)) {
      const metrics = this.calculateMetrics(fontFamily, fontSize, pdfFont);
      this.cache.set(key, metrics);
    }
    
    return this.cache.get(key);
  }

  calculateMetrics(fontFamily, fontSize, pdfFont) {
    if (pdfFont && typeof pdfFont.heightAtSize === 'function') {
      const ascent = pdfFont.heightAtSize(fontSize, { descender: false });
      const height = pdfFont.heightAtSize(fontSize, { descender: true });
      const descent = Math.max(0, height - ascent);

      return {
        ascent,
        descent,
        height,
        baselineOffset: descent,
      };
    }

    throw new Error(`Cannot compute font metrics for "${fontFamily}": embedded font does not support heightAtSize()`);
  }

  // Convert visual Y position to baseline-corrected position
  visualToBaseline(visualY, fontFamily, fontSize, pdfFont) {
    const metrics = this.getMetrics(fontFamily, fontSize, pdfFont);
    return visualY - metrics.ascent;
  }

  // Clear cache (useful for testing or font changes)
  clear() {
    this.cache.clear();
  }
}

// Singleton instance
export const fontMetricsCache = new FontMetricsCache();
