package com.jw.cep.CepTest03.pojo;

import java.io.Serializable;

public class Event implements Serializable {
    private String type;

    private Integer volume;

    private String id;

    public Event() {
    }

    public Event(String type, Integer volume, String id) {
        this.type = type;
        this.volume = volume;
        this.id = id;
    }

    @Override
    public String toString() {
        return "Event{" +
                "type='" + type + '\'' +
                ", volume=" + volume +
                ", id='" + id + '\'' +
                '}';
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getVolume() {
        return volume;
    }

    public void setVolume(Integer volume) {
        this.volume = volume;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
