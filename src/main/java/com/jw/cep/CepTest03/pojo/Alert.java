package com.jw.cep.CepTest03.pojo;

import java.io.Serializable;

public class Alert implements Serializable {
    private String id;
    private String info;

    public Alert() {
    }

    public Alert(String id, String info) {
        this.id = id;
        this.info = info;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "id='" + id + '\'' +
                ", info='" + info + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
