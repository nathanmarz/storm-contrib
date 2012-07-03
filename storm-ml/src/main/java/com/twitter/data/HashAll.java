package com.twitter.data;


public class HashAll extends HashFunction {

    @Override
    public int hash(String key, int seed) {
        return murmurHash32(key, seed);
    }
}
