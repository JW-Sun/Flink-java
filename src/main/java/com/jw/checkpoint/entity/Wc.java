package com.jw.checkpoint.entity;

import java.io.Serializable;

public class Wc implements Serializable {
    private String word;
    private Integer count;

    @Override
    public String toString() {
        String s = "(word = "+word+", count = "+count+")";
        return s;
    }

    public Wc(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public Wc() {
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
