import { customAlphabet } from 'nanoid';

/**
 * Generates an id, e.g. file_Gr0fUrN2z1tK1KJeU4XeV0eSeG9iSkQu
 * @param resource
 * @returns
 */
export default function generateId(resource: string) {
  const alphabet = customAlphabet(
    '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
  );
  return `${resource}_${alphabet(32)}`;
}
