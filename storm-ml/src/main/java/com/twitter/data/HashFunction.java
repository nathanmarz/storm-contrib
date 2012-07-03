package com.twitter.data;

import com.google.common.hash.Hashing;

public abstract class HashFunction {
    public abstract int hash(String key, int seed);

    /**
     * Generates 32 bit hash from byte array and seed using the murmur hash algorithm
     * 
     * @param key
     *            string to hash
     * @param seed
     *            initial seed value
     * @return 32 bit hash of the given string
     */
    protected int murmurHash32(final String key, int seed) {
        int h = Hashing.murmur3_32(seed).hashString(key).asInt();
        h *= (h < 0) ? -1 : 1;
        return h;
    }
}
