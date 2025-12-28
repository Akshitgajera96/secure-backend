import { connection } from '../../queues/vectorQueue.js';

export const getRedisClient = () => {
  return connection || null;
};
