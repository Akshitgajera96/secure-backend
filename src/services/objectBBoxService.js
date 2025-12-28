class ObjectBBoxService {
  setObjectBBox() {
    throw new Error('objectBBoxService is deprecated. Backend object size is authoritative and computed internally.');
  }

  getObjectBBox() {
    throw new Error('objectBBoxService is deprecated. Backend object size is authoritative and computed internally.');
  }

  clearCache() {
    throw new Error('objectBBoxService is deprecated. Backend object size is authoritative and computed internally.');
  }
}

export default new ObjectBBoxService();
